package volume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/uri"
	"github.com/bpalermo/chandler/csi/internal/atomic"
)

// Cache is the node cache, as a volume uses it.
type Cache interface {
	// Put stores a verified bundle as the newest version of a URI.
	Put(uri, version string, b *chandlerv1.Bundle) error
	// Use marks an already held version as the newest of a URI.
	Use(uri, version string) error
	// Get returns a held bundle, verified against its version.
	Get(version string) (*chandlerv1.Bundle, error)
	// Versions returns the versions held for a URI, newest first.
	Versions(uri string) []string
	// Stats returns the number of URIs with held versions and the bytes of
	// content on disk.
	Stats() (uris int, bytes int64)
}

const (
	// dataDir is the directory of a volume that is mounted into the pod.
	dataDir  = "data"
	metaFile = "meta.json"
	// currentLink is the atomic writer's link to the current generation.
	currentLink = "..data"

	dirMode  = 0o755
	metaMode = 0o600
)

// limits is what the driver accepts regardless of the server's own limits,
// which are lower: a bundle past these could not have been sent in one
// message or is not a config bundle at all.
var limits = bundle.Limits{MaxBytes: bundle.HardMaxBytes, MaxFiles: 1 << 14}

// Workload identifies the pod a volume belongs to.
type Workload struct {
	Namespace      string `json:"namespace"`
	PodName        string `json:"podName"`
	PodUID         string `json:"podUid"`
	ServiceAccount string `json:"serviceAccount"`
}

// Meta is what the driver must remember about a volume across restarts. It is
// stored beside the volume's data.
type Meta struct {
	VolumeID   string   `json:"volumeId"`
	TargetPath string   `json:"targetPath"`
	Workload   Workload `json:"workload"`
	Spec       Spec     `json:"spec"`
	// Applied is the version written for each source, "" for none.
	Applied []string `json:"applied"`

	// The rest is kept only for volumes with application acknowledgement.

	// Loaded is the last version of each source the application reported
	// loaded, "" for none: what a rejected version is reverted to.
	Loaded []string `json:"loaded,omitempty"`
	// Rejected is the versions of each source the application rejected. They
	// are never written to this volume again.
	Rejected [][]string `json:"rejected,omitempty"`
	// Generations is the last two generations written, newest first. A
	// report names one of them.
	Generations []Generation `json:"generations,omitempty"`
}

// Generation is one combination of source versions that was written.
type Generation struct {
	ID string `json:"id"`
	// Versions is the version of each source in the generation.
	Versions []string `json:"versions"`
}

// Volume is one pod volume. Its methods are safe for concurrent use.
type Volume struct {
	dir    string
	writer *atomic.Writer
	cache  Cache
	// cacheKeys is each source's key in the node cache (see cacheKey).
	cacheKeys []string

	mu   sync.Mutex
	meta Meta
	// files is what each source currently contributes, keyed by path in the
	// volume. A new generation is the union of all of them.
	files []map[string]atomic.File
	// failures is the server's reason for having no version of a source.
	failures []*status.Status
	// changed is closed and replaced whenever a source gets an answer.
	changed chan struct{}
	// closed is set when the volume is being removed.
	closed bool
	// token is the pod's service account token, as kubelet last supplied
	// it. It is kept in memory only.
	token string
	// overdue is called when the application is late to report on a version;
	// awaiting is the clock running for each source.
	overdue  func(Outcome)
	awaiting []*time.Timer
	// late is the version of each source the application is overdue on.
	late []string
	// sentToken is the token the volume's subscriptions were last made with,
	// and sentAt when.
	sentToken string
	sentAt    time.Time
}

// retryRefused is how often a subscription the server refused for want of a
// valid token is made again with the same token. The refusal may have been
// the server's doing: it could not fetch the cluster's signing keys just then.
const retryRefused = 30 * time.Second

// SetToken records the pod's service account token and reports whether it
// changed.
func (v *Volume) SetToken(token string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	changed := token != v.token
	v.token = token
	return changed
}

// TokenForSubscribe returns the pod's service account token to subscribe
// with, "" when kubelet has not supplied one since the driver started, and
// remembers that it was the one used.
func (v *Volume) TokenForSubscribe() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sentToken, v.sentAt = v.token, time.Now()
	return v.token
}

// NeedsToken reports whether the server refused a subscription of the volume
// for want of a valid token, and subscribing again may succeed: the volume
// now has another token than the one it subscribed with, or enough time has
// passed to try the same one again.
//
// It compares with the token that was sent, not with whether the token just
// changed: a token can arrive after a subscription went out without one and
// before the server's refusal comes back.
func (v *Volume) NeedsToken() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.token == v.sentToken && (v.token == "" || time.Since(v.sentAt) < retryRefused) {
		return false
	}
	for _, failure := range v.failures {
		if codes.Code(failure.GetCode()) == codes.Unauthenticated {
			return true
		}
	}
	return false
}

// Create makes a new, empty volume in dir, which must not hold one.
func Create(dir string, meta Meta, cache Cache) (*Volume, error) {
	if err := os.MkdirAll(filepath.Join(dir, dataDir), dirMode); err != nil {
		return nil, fmt.Errorf("create volume directory: %w", err)
	}
	meta.Applied = make([]string, len(meta.Spec.Sources))
	meta.Loaded = make([]string, len(meta.Spec.Sources))
	meta.Rejected = make([][]string, len(meta.Spec.Sources))
	v := newVolume(dir, meta, cache)
	if err := v.saveLocked(); err != nil {
		return nil, err
	}
	return v, nil
}

// Load restores the volume stored in dir, as Create and later updates left
// it. What each source contributes is read back from the current generation,
// so the next update of one source can be composed with the others.
func Load(dir string, cache Cache) (*Volume, error) {
	raw, err := os.ReadFile(filepath.Join(dir, metaFile))
	if err != nil {
		return nil, err
	}
	var meta Meta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("read %s: %w", metaFile, err)
	}
	if len(meta.Applied) != len(meta.Spec.Sources) {
		return nil, fmt.Errorf("read %s: %d sources but %d applied versions", metaFile, len(meta.Spec.Sources), len(meta.Applied))
	}
	if len(meta.Loaded) != len(meta.Spec.Sources) {
		meta.Loaded = make([]string, len(meta.Spec.Sources))
	}
	if len(meta.Rejected) != len(meta.Spec.Sources) {
		meta.Rejected = make([][]string, len(meta.Spec.Sources))
	}
	v := newVolume(dir, meta, cache)
	for i, source := range meta.Spec.Sources {
		if meta.Applied[i] == "" {
			continue
		}
		files, err := readBack(filepath.Join(dir, dataDir, currentLink), source)
		if err != nil {
			// Forget the version: the server sends the bundle again.
			v.meta.Applied[i] = ""
			continue
		}
		v.files[i] = files
	}
	return v, nil
}

func newVolume(dir string, meta Meta, cache Cache) *Volume {
	n := len(meta.Spec.Sources)
	keys := make([]string, n)
	for i, source := range meta.Spec.Sources {
		canonical := source.URI
		// The spec was validated when the volume was created; the canonical
		// form makes two spellings of a URI share their cached versions.
		if u, err := uri.Parse(source.URI); err == nil {
			canonical = u.String()
		}
		keys[i] = cacheKey(canonical, meta.Workload)
	}
	return &Volume{
		dir:       dir,
		writer:    atomic.NewWriter(filepath.Join(dir, dataDir)),
		cache:     cache,
		cacheKeys: keys,
		meta:      meta,
		files:     make([]map[string]atomic.File, n),
		failures:  make([]*status.Status, n),
		awaiting:  make([]*time.Timer, n),
		late:      make([]string, n),
		changed:   make(chan struct{}),
	}
}

// readBack reads what a source contributes to the generation at root.
func readBack(root string, source Source) (map[string]atomic.File, error) {
	files := make(map[string]atomic.File)
	base := filepath.Join(root, filepath.FromSlash(source.Path))
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		content, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = atomic.File{Mode: info.Mode().Perm(), Content: content}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) && source.IsDir() {
		// A directory bundle with no files leaves nothing on disk.
		return files, nil
	}
	return files, err
}

// Meta returns a copy of the volume's stored description.
func (v *Volume) Meta() Meta {
	v.mu.Lock()
	defer v.mu.Unlock()
	meta := v.meta
	meta.Applied = append([]string(nil), v.meta.Applied...)
	return meta
}

// Memory reports whether the volume is kept in memory.
func (v *Volume) Memory() bool { return v.meta.Spec.Memory }

// MemorySize returns how large the in-memory filesystem of a volume with the
// given spec has to be: every source at its largest, in the current
// generation and the one kept before it.
func MemorySize(spec Spec) uint64 {
	return uint64(len(spec.Sources))*2*bundle.HardMaxBytes + 1<<20
}

// DataDir is the directory that is mounted into the pod.
func (v *Volume) DataDir() string { return filepath.Join(v.dir, dataDir) }

// SubscriptionID returns the identifier of source i's subscription.
func (v *Volume) SubscriptionID(i int) string {
	return fmt.Sprintf("%s/%d", v.meta.VolumeID, i)
}

// Applied returns the version written for source i, "" for none.
func (v *Volume) Applied(i int) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.meta.Applied[i]
}

// Apply writes an update for source i into the volume. A nil error means the
// version is written and is the source's last known good; the caller ACKs. An
// error carries the gRPC code to NACK with, and leaves the volume exactly as
// it was.
//
// The content of the files is never opened or interpreted.
func (v *Volume) Apply(i int, update *chandlerv1.ConfigUpdate) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return grpcstatus.Error(codes.FailedPrecondition, "the volume is being removed")
	}
	source := v.meta.Spec.Sources[i]
	version := update.GetVersion()

	if !update.HasBundle() && version != "" && version == v.meta.Applied[i] {
		// UseCached of what is already written: nothing to do.
		v.answeredLocked(i, nil)
		return nil
	}

	b := update.GetBundle()
	cached := !update.HasBundle()
	if cached {
		// UseCached: the bytes come from the node cache, which verifies
		// them against the version.
		var err error
		if b, err = v.cache.Get(version); err != nil {
			return grpcstatus.Errorf(codes.DataLoss, "%v", err)
		}
	} else if !bundle.Verify(b, version) {
		return grpcstatus.Errorf(codes.DataLoss, "the bundle does not match its version %s", version)
	}
	if update.GetCacheControl().GetNoStore() && !v.meta.Spec.Memory {
		// The provider says this content must never be written to
		// persistent storage, and this volume is on the node's disk.
		return grpcstatus.Errorf(codes.FailedPrecondition,
			"%s is marked no_store and is only written to a volume kept in memory: set the volume attribute %s: %s",
			source.URI, mediumKey, MediumMemory)
	}
	if slices.Contains(v.meta.Rejected[i], version) {
		return grpcstatus.Errorf(codes.FailedPrecondition, "the application rejected version %s", version)
	}
	contribution, err := contribute(source, b)
	if err != nil {
		return err
	}

	// Stage: the bundle goes into the node cache before it is written, so a
	// version in a volume is always one the node can apply again. Content
	// marked no_store never touches the cache.
	switch {
	case cached:
		err = v.cache.Use(v.cacheKeys[i], version)
	case !update.GetCacheControl().GetNoStore():
		err = v.cache.Put(v.cacheKeys[i], version, b)
	}
	if err != nil {
		return grpcstatus.Errorf(writeErrorCode(err), "%v", err)
	}
	return v.writeLocked(i, version, contribution)
}

// contribute returns what a bundle puts into the volume for a source: its
// files, keyed by their path in the volume.
func contribute(source Source, b *chandlerv1.Bundle) (map[string]atomic.File, error) {
	if err := bundle.Validate(b, limits); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}
	contribution := make(map[string]atomic.File, len(b.GetFiles()))
	if source.IsDir() {
		for _, f := range b.GetFiles() {
			contribution[source.Path+"/"+f.GetPath()] = atomic.File{Mode: bundle.Mode(f), Content: f.GetContent()}
		}
		return contribution, nil
	}
	// One object: the source's path names the file, whatever the object
	// is called at the origin.
	if n := len(b.GetFiles()); n != 1 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument,
			"%s names one object but the bundle has %d files", source.URI, n)
	}
	f := b.GetFiles()[0]
	contribution[source.Path] = atomic.File{Mode: bundle.Mode(f), Content: f.GetContent()}
	return contribution, nil
}

// writeLocked swaps in a generation in which source i is at version, with the
// given files, and every other source is as it was.
func (v *Volume) writeLocked(i int, version string, contribution map[string]atomic.File) error {
	// A generation is the whole volume: this source's new files and what
	// every other source already contributes.
	generation := make(map[string]atomic.File)
	for j, files := range v.files {
		if j == i {
			files = contribution
		}
		for p, f := range files {
			generation[p] = f
		}
	}
	versions := append([]string(nil), v.meta.Applied...)
	versions[i] = version

	var reserved map[string]atomic.File
	next := Generation{ID: generationID(versions), Versions: versions}
	if v.meta.Spec.AppAck {
		// The manifest travels in the generation it describes, so the
		// application never reads one that is about other files.
		raw, err := v.manifestFile(next)
		if err != nil {
			return grpcstatus.Errorf(codes.Internal, "%v", err)
		}
		reserved = map[string]atomic.File{ManifestName: {Mode: 0o444, Content: raw}}
	}
	if err := v.writer.WriteWith(generation, reserved); err != nil {
		return grpcstatus.Errorf(writeErrorCode(err), "%v", err)
	}

	v.files[i] = contribution
	changed := v.meta.Applied[i] != version
	v.meta.Applied[i] = version
	if changed {
		v.awaitReportLocked(i)
	}
	if v.meta.Spec.AppAck && (len(v.meta.Generations) == 0 || v.meta.Generations[0].ID != next.ID) {
		v.meta.Generations = append([]Generation{next}, v.meta.Generations...)
		if len(v.meta.Generations) > 2 {
			v.meta.Generations = v.meta.Generations[:2]
		}
	}
	v.answeredLocked(i, nil)
	// The files are swapped in whether or not this succeeds; a stale record
	// only costs a resend after a driver restart.
	_ = v.saveLocked()
	return nil
}

// Close stops the volume from being written, waiting for a write in progress.
// The caller can then remove its directory.
func (v *Volume) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closed = true
	for _, timer := range v.awaiting {
		if timer != nil {
			timer.Stop()
		}
	}
}

// writeErrorCode tells a full disk, which the server retries knowing why,
// from any other I/O error.
func writeErrorCode(err error) codes.Code {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return codes.ResourceExhausted
	}
	return codes.Internal
}

// cacheKey is the node cache's key for a source: its canonical URI, and the
// pod's namespace and service account.
//
// A provider may serve different content for one URI to different pods (its
// vary_on). Of the attributes it may vary on, the cluster, region and zone
// are the same for every pod on a node, and the namespace and service
// account are not. Keying by them keeps one pod from being given, from the
// cache, the version of a URI the server sent to a pod in another namespace
// or of another service account, which it would not have been sent itself.
func cacheKey(canonicalURI string, w Workload) string {
	return canonicalURI + " " + w.Namespace + "/" + w.ServiceAccount
}

// CachedVersions returns the versions of source i the node cache holds for
// this pod, newest first. The server can name any of them without sending
// bytes.
func (v *Volume) CachedVersions(i int) []string {
	return v.cache.Versions(v.cacheKeys[i])
}

// CacheKeys returns the node cache's key for every source.
func (v *Volume) CacheKeys() []string { return append([]string(nil), v.cacheKeys...) }

// ApplyCached writes, for every source that has nothing written yet, the
// newest version the node cache holds for this pod. It is how a volume is mounted while
// the server cannot be reached. It fails, naming the source, if the cache has
// nothing for one of them.
func (v *Volume) ApplyCached() error {
	for i, source := range v.meta.Spec.Sources {
		if v.Applied(i) != "" {
			continue
		}
		versions := v.cache.Versions(v.cacheKeys[i])
		if len(versions) == 0 {
			return grpcstatus.Errorf(codes.Unavailable, "source %q (%s): nothing is cached on this node", source.Name, source.URI)
		}
		update := chandlerv1.ConfigUpdate_builder{
			Version:   proto.String(versions[0]),
			UseCached: &chandlerv1.UseCached{},
		}.Build()
		if err := v.Apply(i, update); err != nil {
			return grpcstatus.Errorf(codes.Unavailable, "source %q (%s): %v", source.Name, source.URI, err)
		}
	}
	return nil
}

// Failure returns the server's reason for having no version of a source that
// has nothing written, or nil when no source is in that state.
func (v *Volume) Failure() *status.Status {
	v.mu.Lock()
	defer v.mu.Unlock()
	for i := range v.meta.Spec.Sources {
		if v.meta.Applied[i] == "" && v.failures[i] != nil {
			return v.failures[i]
		}
	}
	return nil
}

// Fail records that the server cannot produce a version of source i for now.
func (v *Volume) Fail(i int, st *status.Status) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.answeredLocked(i, st)
}

func (v *Volume) answeredLocked(i int, failure *status.Status) {
	v.failures[i] = failure
	close(v.changed)
	v.changed = make(chan struct{})
}

// WaitReady blocks until every source is written, and fails as soon as the
// server reports it has no version of one, or when ctx ends. The error is a
// gRPC status.
func (v *Volume) WaitReady(ctx context.Context) error {
	for {
		v.mu.Lock()
		waiting := ""
		for i, source := range v.meta.Spec.Sources {
			if v.meta.Applied[i] != "" {
				continue
			}
			if st := v.failures[i]; st != nil {
				v.mu.Unlock()
				return grpcstatus.Errorf(codes.Code(st.GetCode()), "source %q (%s): %s", source.Name, source.URI, st.GetMessage())
			}
			if waiting == "" {
				waiting = fmt.Sprintf("source %q (%s)", source.Name, source.URI)
			}
		}
		changed := v.changed
		v.mu.Unlock()
		if waiting == "" {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return grpcstatus.Errorf(codes.Unavailable, "no answer from the config server for %s", waiting)
		}
	}
}

// saveLocked stores the volume's description, replacing the file atomically.
func (v *Volume) saveLocked() error {
	raw, err := json.Marshal(v.meta)
	if err != nil {
		return err
	}
	tmp := filepath.Join(v.dir, metaFile+".tmp")
	if err := os.WriteFile(tmp, raw, metaMode); err != nil {
		return fmt.Errorf("write %s: %w", metaFile, err)
	}
	if err := os.Rename(tmp, filepath.Join(v.dir, metaFile)); err != nil {
		return fmt.Errorf("write %s: %w", metaFile, err)
	}
	return nil
}
