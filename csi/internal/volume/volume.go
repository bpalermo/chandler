package volume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/csi/internal/atomic"
)

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
}

// Volume is one pod volume. Its methods are safe for concurrent use.
type Volume struct {
	dir    string
	writer *atomic.Writer

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
}

// Create makes a new, empty volume in dir, which must not hold one.
func Create(dir string, meta Meta) (*Volume, error) {
	if err := os.MkdirAll(filepath.Join(dir, dataDir), dirMode); err != nil {
		return nil, fmt.Errorf("create volume directory: %w", err)
	}
	meta.Applied = make([]string, len(meta.Spec.Sources))
	v := newVolume(dir, meta)
	if err := v.saveLocked(); err != nil {
		return nil, err
	}
	return v, nil
}

// Load restores the volume stored in dir, as Create and later updates left
// it. What each source contributes is read back from the current generation,
// so the next update of one source can be composed with the others.
func Load(dir string) (*Volume, error) {
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
	v := newVolume(dir, meta)
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

func newVolume(dir string, meta Meta) *Volume {
	n := len(meta.Spec.Sources)
	return &Volume{
		dir:      dir,
		writer:   atomic.NewWriter(filepath.Join(dir, dataDir)),
		meta:     meta,
		files:    make([]map[string]atomic.File, n),
		failures: make([]*status.Status, n),
		changed:  make(chan struct{}),
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

	if !update.HasBundle() {
		// UseCached: only the version already written can be reused, as
		// there is no node cache yet.
		if version == "" || version != v.meta.Applied[i] {
			return grpcstatus.Errorf(codes.DataLoss, "version %s is not held on this node", version)
		}
		v.answeredLocked(i, nil)
		return nil
	}

	b := update.GetBundle()
	if !bundle.Verify(b, version) {
		return grpcstatus.Errorf(codes.DataLoss, "the bundle does not match its version %s", version)
	}
	if err := bundle.Validate(b, limits); err != nil {
		return grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}
	contribution := make(map[string]atomic.File, len(b.GetFiles()))
	if source.IsDir() {
		for _, f := range b.GetFiles() {
			contribution[source.Path+"/"+f.GetPath()] = atomic.File{Mode: bundle.Mode(f), Content: f.GetContent()}
		}
	} else {
		// One object: the source's path names the file, whatever the object
		// is called at the origin.
		if n := len(b.GetFiles()); n != 1 {
			return grpcstatus.Errorf(codes.InvalidArgument,
				"%s names one object but the bundle has %d files", source.URI, n)
		}
		f := b.GetFiles()[0]
		contribution[source.Path] = atomic.File{Mode: bundle.Mode(f), Content: f.GetContent()}
	}

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
	if err := v.writer.Write(generation); err != nil {
		code := codes.Internal
		if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
			code = codes.ResourceExhausted
		}
		return grpcstatus.Errorf(code, "%v", err)
	}

	v.files[i] = contribution
	v.meta.Applied[i] = version
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
