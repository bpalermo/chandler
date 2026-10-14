package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/csi/internal/cache"
	"github.com/bpalermo/chandler/csi/internal/client"
	"github.com/bpalermo/chandler/csi/internal/events"
)

// fakeMounter records mounts instead of performing them.
type fakeMounter struct {
	mounts  map[string]string // target -> source
	memory  map[string]uint64 // in-memory filesystems: target -> size
	failing error
}

func (f *fakeMounter) BindReadOnly(source, target string) error {
	if f.failing != nil {
		return f.failing
	}
	f.mounts[target] = source
	return nil
}

func (f *fakeMounter) Memory(target string, size uint64) error {
	if f.failing != nil {
		return f.failing
	}
	f.memory[target] = size
	f.mounts[target] = "tmpfs"
	return nil
}

func (f *fakeMounter) Unmount(target string) error {
	delete(f.memory, target)
	delete(f.mounts, target)
	return nil
}

func (f *fakeMounter) IsMountPoint(target string) (bool, error) {
	_, ok := f.mounts[target]
	return ok, nil
}

// fakeServer answers subscriptions the way the config server would: with the
// content it holds for the URI, with an error, or not at all.
type fakeServer struct {
	mu      sync.Mutex
	content map[string]string // URI -> content of its one file
	missing map[string]bool   // URIs answered with NOT_FOUND
	// unavailable answers every URI without content with UNAVAILABLE, as a
	// server whose provider is down does.
	unavailable bool
	// noStore marks what is delivered at subscribe as no_store.
	noStore bool
	subs    map[string]client.Subscription
	reports []*chandlerv1.AppReport
	applied map[string]string // subscription ID -> applied version it subscribed with
	down    error
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		content: map[string]string{},
		missing: map[string]bool{},
		subs:    map[string]client.Subscription{},
		applied: map[string]string{},
	}
}

func update(id, content string) *chandlerv1.ConfigUpdate {
	b := chandlerv1.Bundle_builder{Files: []*chandlerv1.File{
		chandlerv1.File_builder{Path: proto.String("origin-name"), Content: []byte(content)}.Build(),
	}}.Build()
	return chandlerv1.ConfigUpdate_builder{
		SubscriptionId: proto.String(id),
		Nonce:          proto.String("1"),
		Version:        proto.String(bundle.Version(b)),
		Bundle:         b,
	}.Build()
}

func (f *fakeServer) Subscribe(s client.Subscription) {
	f.mu.Lock()
	f.subs[s.ID] = s
	f.applied[s.ID] = s.Handler.Applied()
	content, ok := f.content[s.URI]
	missing := f.missing[s.URI]
	unavailable := f.unavailable
	f.mu.Unlock()
	switch {
	case ok && f.noStore:
		_ = s.Handler.Apply(noStore(update(s.ID, content)))
	case ok:
		if err := s.Handler.Apply(update(s.ID, content)); err != nil {
			panic(err)
		}
	case missing:
		s.Handler.Fail(&rpcstatus.Status{Code: int32(codes.NotFound), Message: "no such file"})
	case unavailable:
		s.Handler.Fail(&rpcstatus.Status{Code: int32(codes.Unavailable), Message: "provider file is not connected"})
	}
}

func (f *fakeServer) Unsubscribe(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.subs, id)
}

func (f *fakeServer) Err() error { return f.down }

func (f *fakeServer) Report(r *chandlerv1.AppReport) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, r)
}

func (f *fakeServer) subscribed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}

// recorded collects the events the driver records.
type recorded struct {
	mu     sync.Mutex
	events []string // "<namespace>/<pod> <reason>: <message>"
}

func (r *recorded) Warning(pod events.Pod, reason, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf("%s/%s %s: %s", pod.Namespace, pod.Name, reason, message))
}

// has reports whether an event with the reason, mentioning text, was
// recorded on the test pod.
func (r *recorded) has(reason, text string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if strings.HasPrefix(e, "shop/web-0 "+reason+": ") && strings.Contains(e, text) {
			return true
		}
	}
	return false
}

type fixture struct {
	events *recorded
	cache  *cache.Cache
	driver *Driver
	mounts *fakeMounter
	server *fakeServer
	state  string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	f := fixture{
		mounts: &fakeMounter{mounts: map[string]string{}, memory: map[string]uint64{}},
		server: newFakeServer(),
		state:  t.TempDir(),
		events: &recorded{},
	}
	nodeCache, err := cache.Open(filepath.Join(f.state, "cache"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.cache = nodeCache
	f.server.content["file:///app.yaml"] = "a: 1\n"
	f.server.content["file:///flags.json"] = "{}"
	f.driver = f.restart(t)
	return f
}

// restart returns a driver over the same state directory and mounts, as after
// the driver's pod was replaced.
func (f fixture) restart(t *testing.T) *Driver {
	t.Helper()
	d, err := New(Config{
		NodeID:         "node-1",
		Version:        "test",
		StateDir:       f.state,
		Mounter:        f.mounts,
		Server:         f.server,
		Cache:          f.cache,
		Events:         f.events,
		PublishTimeout: 2 * time.Second,
		CachedAfter:    10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

const twoSources = `
- uri: file:///app.yaml
  path: app.yaml
- uri: file:///flags.json
  path: etc/flags.json
`

func publishRequest(volumeID, target, sources string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: target,
		Readonly:   true,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY},
		},
		VolumeContext: map[string]string{
			ephemeralKey:    "true",
			"sources":       sources,
			podNameKey:      "web-0",
			podNamespaceKey: "shop",
			podUIDKey:       "uid-1",
			tokensKey:       `{"chandler":{"token":"token-1","expirationTimestamp":"2030-01-01T00:00:00Z"},"other":{"token":"not-ours"}}`,
		},
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func TestIdentityAndNodeInfo(t *testing.T) {
	d := newFixture(t).driver
	ctx := context.Background()

	info, err := d.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil || info.GetName() != Name || info.GetVendorVersion() != "test" {
		t.Errorf("GetPluginInfo = %v, %v", info, err)
	}
	probe, err := d.Probe(ctx, &csi.ProbeRequest{})
	if err != nil || !probe.GetReady().GetValue() {
		t.Errorf("Probe = %v, %v", probe, err)
	}
	node, err := d.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil || node.GetNodeId() != "node-1" {
		t.Errorf("NodeGetInfo = %v, %v", node, err)
	}
	caps, err := d.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	if err != nil || len(caps.GetCapabilities()) != 0 {
		t.Errorf("NodeGetCapabilities = %v, %v; want none (no staging)", caps, err)
	}
}

func TestPublishDeliversEverySourceBeforeMounting(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "pods", "uid", "mount")
	data := filepath.Join(f.state, "volumes", "csi-abc", "data")

	if _, err := f.driver.NodePublishVolume(ctx, publishRequest("csi-abc", target, twoSources)); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if got := f.mounts.mounts[target]; got != data {
		t.Errorf("mounted %q at target, want %q", got, data)
	}
	// The source's path names the file, not the object's name at the origin.
	if got := read(t, filepath.Join(data, "app.yaml")); got != "a: 1\n" {
		t.Errorf("app.yaml = %q", got)
	}
	if got := read(t, filepath.Join(data, "etc", "flags.json")); got != "{}" {
		t.Errorf("etc/flags.json = %q", got)
	}
	sub := f.server.subs["csi-abc/0"]
	if w := sub.Workload; w.GetNamespace() != "shop" || w.GetPodName() != "web-0" || w.GetPodUid() != "uid-1" {
		t.Errorf("subscribed as %v, want the pod's identity", w)
	}

	// Publishing again is a no-op.
	generation, _ := os.Readlink(filepath.Join(data, "..data"))
	if _, err := f.driver.NodePublishVolume(ctx, publishRequest("csi-abc", target, twoSources)); err != nil {
		t.Fatalf("second NodePublishVolume: %v", err)
	}
	if again, _ := os.Readlink(filepath.Join(data, "..data")); again != generation {
		t.Errorf("second publish rewrote the volume: %q -> %q", generation, again)
	}

	// An update to one source keeps the other.
	if err := sub.Handler.Apply(update("csi-abc/0", "a: 2\n")); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := read(t, filepath.Join(data, "app.yaml")); got != "a: 2\n" {
		t.Errorf("app.yaml after update = %q", got)
	}
	if got := read(t, filepath.Join(data, "etc", "flags.json")); got != "{}" {
		t.Errorf("etc/flags.json after the other source's update = %q", got)
	}

	unpublish := &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-abc", TargetPath: target}
	if _, err := f.driver.NodeUnpublishVolume(ctx, unpublish); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if len(f.mounts.mounts) != 0 {
		t.Errorf("mounts left behind: %v", f.mounts.mounts)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Errorf("%d subscriptions left behind", n)
	}
	for _, path := range []string{target, filepath.Join(f.state, "volumes", "csi-abc")} {
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s still exists: %v", path, err)
		}
	}
	// A write that was already on its way must not bring the directory back.
	if err := sub.Handler.Apply(update("csi-abc/0", "late")); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("update after unpublish = %v, want FailedPrecondition", err)
	}

	// Unpublishing again is a no-op.
	if _, err := f.driver.NodeUnpublishVolume(ctx, unpublish); err != nil {
		t.Errorf("second NodeUnpublishVolume: %v", err)
	}
}

// A volume is mounted complete or not at all, and a failed attempt leaves
// nothing for the retry to trip over.
func TestPublishFailsWithoutMounting(t *testing.T) {
	for name, tc := range map[string]struct {
		prepare func(*fakeServer)
		code    codes.Code
		message string
	}{
		"a source the provider does not have": {
			prepare: func(s *fakeServer) { delete(s.content, "file:///flags.json"); s.missing["file:///flags.json"] = true },
			code:    codes.NotFound,
			message: "file:///flags.json",
		},
		"a source that gets no answer": {
			prepare: func(s *fakeServer) { delete(s.content, "file:///flags.json") },
			code:    codes.Unavailable,
			message: "no answer from the config server",
		},
		"an unreachable server": {
			prepare: func(s *fakeServer) {
				s.content = map[string]string{}
				s.down = errors.New("config server chandler:9000 is unreachable: connection refused")
			},
			code:    codes.Unavailable,
			message: "connection refused",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			tc.prepare(f.server)
			target := filepath.Join(t.TempDir(), "mount")

			_, err := f.driver.NodePublishVolume(context.Background(), publishRequest("csi-abc", target, twoSources))
			if status.Code(err) != tc.code || !strings.Contains(err.Error(), tc.message) {
				t.Errorf("got %v, want %s mentioning %q", err, tc.code, tc.message)
			}
			if len(f.mounts.mounts) != 0 {
				t.Errorf("mounted a partial volume: %v", f.mounts.mounts)
			}
			if n := f.server.subscribed(); n != 0 {
				t.Errorf("%d subscriptions left behind", n)
			}
			if entries, _ := os.ReadDir(filepath.Join(f.state, "volumes")); len(entries) != 0 {
				t.Errorf("volume directory left behind: %v", entries)
			}

			// The retry succeeds once the cause is gone.
			f.server.down = nil
			f.server.content["file:///app.yaml"] = "a: 1\n"
			f.server.content["file:///flags.json"] = "{}"
			if _, err := f.driver.NodePublishVolume(context.Background(), publishRequest("csi-abc", target, twoSources)); err != nil {
				t.Errorf("retry: %v", err)
			}
		})
	}
}

func TestPublishRejectsBadRequests(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mount")

	tests := map[string]func(*csi.NodePublishVolumeRequest){
		"no volume ID":       func(r *csi.NodePublishVolumeRequest) { r.VolumeId = "" },
		"escaping volume ID": func(r *csi.NodePublishVolumeRequest) { r.VolumeId = "../../etc" },
		"dot-dot volume ID":  func(r *csi.NodePublishVolumeRequest) { r.VolumeId = ".." },
		"no target":          func(r *csi.NodePublishVolumeRequest) { r.TargetPath = "" },
		"no capability":      func(r *csi.NodePublishVolumeRequest) { r.VolumeCapability = nil },
		"block volume": func(r *csi.NodePublishVolumeRequest) {
			r.VolumeCapability.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
		},
		"not ephemeral": func(r *csi.NodePublishVolumeRequest) { r.VolumeContext = nil },
		"writable":      func(r *csi.NodePublishVolumeRequest) { r.Readonly = false },
		"no sources":    func(r *csi.NodePublishVolumeRequest) { delete(r.VolumeContext, "sources") },
		"bad sources":   func(r *csi.NodePublishVolumeRequest) { r.VolumeContext["sources"] = "- uri: nope\n  path: x\n" },
	}
	for name, mutate := range tests {
		req := publishRequest("csi-abc", target, twoSources)
		mutate(req)
		_, err := f.driver.NodePublishVolume(ctx, req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
	if len(f.mounts.mounts) != 0 {
		t.Errorf("a rejected request mounted something: %v", f.mounts.mounts)
	}
	entries, _ := os.ReadDir(filepath.Join(f.state, "volumes"))
	if len(entries) != 0 {
		t.Errorf("a rejected request left a volume directory: %v", entries)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Errorf("a rejected request subscribed: %d", n)
	}

	for _, id := range []string{"", "..", "a/b"} {
		_, err := f.driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: id, TargetPath: target})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("unpublish %q: got %v, want InvalidArgument", id, err)
		}
	}
}

func TestPublishMountFailure(t *testing.T) {
	f := newFixture(t)
	f.mounts.failing = errors.New("boom")
	_, err := f.driver.NodePublishVolume(context.Background(),
		publishRequest("csi-abc", filepath.Join(t.TempDir(), "mount"), twoSources))
	if status.Code(err) != codes.Internal {
		t.Errorf("got %v, want Internal", err)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Errorf("%d subscriptions left behind", n)
	}
}

// A driver restart must not stop updates reaching the pods that are running.
func TestRestoreResumesMountedVolumes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	mounted := filepath.Join(t.TempDir(), "mounted")
	gone := filepath.Join(t.TempDir(), "gone")
	for id, target := range map[string]string{"csi-mounted": mounted, "csi-gone": gone} {
		if _, err := f.driver.NodePublishVolume(ctx, publishRequest(id, target, twoSources)); err != nil {
			t.Fatal(err)
		}
	}
	// The node lost one mount while the driver was away, and a publish was
	// interrupted before it recorded anything.
	delete(f.mounts.mounts, gone)
	if err := os.MkdirAll(filepath.Join(f.state, "volumes", "csi-interrupted", "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	version := f.server.subs["csi-mounted/0"].Handler.Applied()

	f.server.subs, f.server.applied = map[string]client.Subscription{}, map[string]string{}
	restarted := f.restart(t)
	if err := restarted.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if n := f.server.subscribed(); n != 2 {
		t.Fatalf("%d subscriptions after restore, want the mounted volume's 2", n)
	}
	if got := f.server.applied["csi-mounted/0"]; got != version || got == "" {
		t.Errorf("resubscribed with applied version %q, want %q", got, version)
	}
	for _, id := range []string{"csi-gone", "csi-interrupted"} {
		if _, err := os.Stat(filepath.Join(f.state, "volumes", id)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("stale volume %s was kept: %v", id, err)
		}
	}

	// An update to one source after the restart still composes with the
	// other, whose bytes were read back from disk.
	data := filepath.Join(f.state, "volumes", "csi-mounted", "data")
	if err := f.server.subs["csi-mounted/0"].Handler.Apply(update("csi-mounted/0", "a: 3\n")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, filepath.Join(data, "app.yaml")); got != "a: 3\n" {
		t.Errorf("app.yaml = %q", got)
	}
	if got := read(t, filepath.Join(data, "etc", "flags.json")); got != "{}" {
		t.Errorf("etc/flags.json was lost across the restart: %q", got)
	}
}

// With failurePolicy UseCached a pod starts from what the node holds when the
// server cannot supply a source; with the default policy it waits.
func TestPublishFromTheNodeCache(t *testing.T) {
	outages := map[string]func(*fakeServer){
		"unreachable server": func(s *fakeServer) {
			s.content = map[string]string{}
			s.down = errors.New("config server is unreachable: connection refused")
		},
		"provider down": func(s *fakeServer) {
			s.content = map[string]string{}
			s.unavailable = true
		},
		"no answer in time": func(s *fakeServer) { s.content = map[string]string{} },
	}
	for name, outage := range outages {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			// A first pod fills the cache, then goes away.
			warm := filepath.Join(t.TempDir(), "warm")
			if _, err := f.driver.NodePublishVolume(ctx, publishRequest("csi-warm", warm, twoSources)); err != nil {
				t.Fatal(err)
			}
			if _, err := f.driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-warm", TargetPath: warm}); err != nil {
				t.Fatal(err)
			}
			outage(f.server)

			strict := publishRequest("csi-strict", filepath.Join(t.TempDir(), "strict"), twoSources)
			if _, err := f.driver.NodePublishVolume(ctx, strict); status.Code(err) != codes.Unavailable {
				t.Errorf("failurePolicy Fail: got %v, want Unavailable", err)
			}

			target := filepath.Join(t.TempDir(), "cached")
			lenient := publishRequest("csi-cached", target, twoSources)
			lenient.VolumeContext["failurePolicy"] = "UseCached"
			if _, err := f.driver.NodePublishVolume(ctx, lenient); err != nil {
				t.Fatalf("failurePolicy UseCached: %v", err)
			}
			data := f.mounts.mounts[target]
			if got := read(t, filepath.Join(data, "app.yaml")); got != "a: 1\n" {
				t.Errorf("app.yaml from the cache = %q", got)
			}

			// It stays subscribed, so it converges when the server answers.
			sub, ok := f.server.subs["csi-cached/0"]
			if !ok {
				t.Fatal("the volume mounted from the cache is not subscribed")
			}
			if err := sub.Handler.Apply(update("csi-cached/0", "a: 2\n")); err != nil {
				t.Fatal(err)
			}
			if got := read(t, filepath.Join(data, "app.yaml")); got != "a: 2\n" {
				t.Errorf("app.yaml after the server returned = %q", got)
			}

			// What was cached for one namespace is not given to a pod of
			// another: a provider may serve the same URI differently to it.
			elsewhere := publishRequest("csi-elsewhere", filepath.Join(t.TempDir(), "elsewhere"), twoSources)
			elsewhere.VolumeContext["failurePolicy"] = "UseCached"
			elsewhere.VolumeContext[podNamespaceKey] = "billing"
			if _, err := f.driver.NodePublishVolume(ctx, elsewhere); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "nothing is cached") {
				t.Errorf("another namespace: got %v, want Unavailable, nothing cached for it", err)
			}

			// Nothing cached for a URI: UseCached cannot help.
			cold := publishRequest("csi-cold", filepath.Join(t.TempDir(), "cold"), "- uri: file:///never-seen.yaml\n  path: x\n")
			cold.VolumeContext["failurePolicy"] = "UseCached"
			if _, err := f.driver.NodePublishVolume(ctx, cold); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "nothing is cached") {
				t.Errorf("with nothing cached: got %v, want Unavailable saying so", err)
			}
		})
	}
}

func TestNewValidatesConfig(t *testing.T) {
	m, s, c := &fakeMounter{mounts: map[string]string{}, memory: map[string]uint64{}}, newFakeServer(), newFixture(t).cache
	for name, cfg := range map[string]Config{
		"no node ID":   {StateDir: t.TempDir(), Mounter: m, Server: s, Cache: c},
		"no state dir": {NodeID: "n", Mounter: m, Server: s, Cache: c},
		"no mounter":   {NodeID: "n", StateDir: t.TempDir(), Server: s, Cache: c},
		"no server":    {NodeID: "n", StateDir: t.TempDir(), Mounter: m, Cache: c},
		"no cache":     {NodeID: "n", StateDir: t.TempDir(), Mounter: m, Server: s},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

// shortSocket returns a path to a volume's acknowledgement socket that fits
// in a socket address, as an application has it: from inside the volume.
func shortSocket(t *testing.T, socket string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "v")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	link := filepath.Join(dir, "vol")
	if err := os.Symlink(filepath.Dir(socket), link); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(link, filepath.Base(socket))
}

// call sends one HTTP request to a volume's acknowledgement socket.
func call(t *testing.T, socket, method, path, body string) (int, string) {
	t.Helper()
	short := shortSocket(t, socket)
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", short)
		},
	}}
	defer client.CloseIdleConnections()
	req, err := http.NewRequest(method, "http://chandler"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// firstDetail returns the first detail of a status, nil if it has none.
func firstDetail(st *status.Status) any {
	if details := st.Details(); len(details) > 0 {
		return details[0]
	}
	return nil
}

// dialApp connects to a volume's acknowledgement socket as a gRPC client.
func dialApp(t *testing.T, socket string) chandlerv1.ApplicationServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("unix://"+shortSocket(t, socket), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return chandlerv1.NewApplicationServiceClient(conn)
}

// A volume with appAck gets a socket the application reports on; a rejection
// puts the last loaded version back at once and reaches the server.
func TestApplicationAcknowledgement(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mount")
	req := publishRequest("csi-ack", target, "- uri: file:///app.yaml\n  path: app.yaml\n")
	req.VolumeContext["appAck"] = "true"
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	data := f.mounts.mounts[target]
	socket := filepath.Join(data, "..chandler.sock")

	// The manifest file and the API agree on the generation.
	var manifest, config struct {
		Generation string
		Sources    []struct{ Name, Version, State string }
	}
	if err := json.Unmarshal([]byte(read(t, filepath.Join(data, "..chandler.json"))), &manifest); err != nil {
		t.Fatal(err)
	}
	code, body := call(t, socket, http.MethodGet, "/v1/config", "")
	if err := json.Unmarshal([]byte(body), &config); err != nil || code != http.StatusOK {
		t.Fatalf("GET /v1/config = %d %s (%v)", code, body, err)
	}
	if config.Generation == "" || config.Generation != manifest.Generation ||
		config.Sources[0].State != "STATE_PENDING" || manifest.Sources[0].State != "" {
		t.Fatalf("config = %+v, manifest = %+v; want the same generation, pending in the API only", config, manifest)
	}
	first := config.Sources[0].Version

	// The same service over gRPC, on the same socket.
	app := dialApp(t, socket)
	viaGRPC, err := app.GetConfig(ctx, &chandlerv1.GetConfigRequest{})
	if err != nil {
		t.Fatalf("GetConfig over gRPC: %v", err)
	}
	if m := viaGRPC.GetConfig(); m.GetGeneration() != config.Generation || len(m.GetSources()) != 1 ||
		m.GetSources()[0].GetVersion() != first || m.GetSources()[0].GetState() != chandlerv1.Manifest_STATE_PENDING {
		t.Errorf("GetConfig over gRPC = %v, want what HTTP returned", m)
	}

	if code, body := call(t, socket, http.MethodPost, "/v1/reports", `{"generation":"`+config.Generation+`","state":"STATE_LOADED"}`); code != http.StatusNoContent || body != "" {
		t.Fatalf("reporting loaded = %d %q, want 204 and no body", code, body)
	}

	// A new version arrives and the application refuses it.
	sub := f.server.subs["csi-ack/0"]
	if !sub.AppAck || sub.Handler.Loaded() != first {
		t.Errorf("subscription AppAck=%v loaded=%q, want true and the first version", sub.AppAck, sub.Handler.Loaded())
	}
	second := update("csi-ack/0", "a: broken\n")
	if err := sub.Handler.Apply(second); err != nil {
		t.Fatal(err)
	}
	_, body = call(t, socket, http.MethodGet, "/v1/config", "")
	if err := json.Unmarshal([]byte(body), &config); err != nil {
		t.Fatal(err)
	}
	report := `{"generation":"` + config.Generation + `","sources":[{"name":"app.yaml","state":"STATE_REJECTED","message":"cannot parse"}]}`
	if code, body := call(t, socket, http.MethodPost, "/v1/reports", report); code != http.StatusNoContent {
		t.Fatalf("reporting rejected = %d %s", code, body)
	}
	// What the application sees now: the version it loaded, in a new
	// generation.
	reverted, err := app.GetConfig(ctx, &chandlerv1.GetConfigRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if src := reverted.GetConfig().GetSources()[0]; src.GetVersion() != first || src.GetState() != chandlerv1.Manifest_STATE_LOADED {
		t.Errorf("after the rejection the config is %v, want the first version, loaded", reverted.GetConfig())
	}
	if got := read(t, filepath.Join(data, "app.yaml")); got != "a: 1\n" {
		t.Errorf("after the rejection the file reads %q, want the last loaded version", got)
	}
	// The server is told, with what is in the volume now.
	f.server.mu.Lock()
	reports := f.server.reports
	f.server.mu.Unlock()
	if len(reports) != 2 {
		t.Fatalf("%d reports reached the server, want 2", len(reports))
	}
	if r := reports[1]; r.GetState() != chandlerv1.AppReport_STATE_REJECTED || r.GetVersion() != second.GetVersion() ||
		r.GetAppliedVersion() != first || r.GetMessage() != "cannot parse" || r.GetSubscriptionId() != "csi-ack/0" {
		t.Errorf("rejection report = %v", r)
	}
	// The rejected version is never written to this volume again.
	if err := sub.Handler.Apply(second); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("applying the rejected version again = %v, want FailedPrecondition", err)
	}

	// Errors an application can act on.
	code, body = call(t, socket, http.MethodPost, "/v1/reports", `{"generation":"g-0000","state":"STATE_LOADED"}`)
	if code != http.StatusConflict || !strings.Contains(body, reverted.GetConfig().GetGeneration()) {
		t.Errorf("a stale generation = %d %s, want 409 with the current manifest", code, body)
	}
	for name, c := range map[string]struct {
		body string
		want int
	}{
		"an unknown source":    {`{"generation":"` + config.Generation + `","sources":[{"name":"nope","state":"STATE_LOADED"}]}`, http.StatusNotFound},
		"an unknown state":     {`{"generation":"` + config.Generation + `","state":"fine"}`, http.StatusBadRequest},
		"a state not reported": {`{"generation":"` + config.Generation + `","state":"STATE_PENDING"}`, http.StatusBadRequest},
		"no state":             {`{"generation":"` + config.Generation + `"}`, http.StatusBadRequest},
		"an unknown field":     {`{"generation":"` + config.Generation + `","state":"STATE_LOADED","verdict":"fine"}`, http.StatusBadRequest},
		"a malformed body":     {`not json`, http.StatusBadRequest},
	} {
		if code, body := call(t, socket, http.MethodPost, "/v1/reports", c.body); code != c.want {
			t.Errorf("%s = %d %s, want %d", name, code, body, c.want)
		}
	}
	// The same over gRPC, with the current manifest in the status.
	_, err = app.Report(ctx, chandlerv1.ReportRequest_builder{
		Generation: proto.String("g-0000"),
		State:      chandlerv1.Manifest_STATE_LOADED.Enum(),
	}.Build())
	st := status.Convert(err)
	current, _ := firstDetail(st).(*chandlerv1.Manifest)
	if st.Code() != codes.Aborted || current.GetGeneration() != reverted.GetConfig().GetGeneration() {
		t.Errorf("a stale generation over gRPC = %v with %v, want Aborted with the current manifest", err, current)
	}
	_, err = app.Report(ctx, chandlerv1.ReportRequest_builder{
		Generation: proto.String(config.Generation),
		Sources: []*chandlerv1.SourceReport{chandlerv1.SourceReport_builder{
			Name:  proto.String("nope"),
			State: chandlerv1.Manifest_STATE_LOADED.Enum(),
		}.Build()},
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("an unknown source over gRPC = %v, want NotFound", err)
	}

	// A restarted driver serves the socket again and remembers the verdicts.
	restarted := f.restart(t)
	if err := restarted.Restore(); err != nil {
		t.Fatal(err)
	}
	code, body = call(t, socket, http.MethodGet, "/v1/config", "")
	if err := json.Unmarshal([]byte(body), &config); err != nil || code != http.StatusOK || config.Sources[0].State != "STATE_LOADED" {
		t.Errorf("after a restart GET /v1/config = %d %s; want the source loaded", code, body)
	}

	if _, err := restarted.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-ack", TargetPath: target}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the socket outlived the volume: %v", err)
	}
}

// A volume that did not ask gets neither the manifest nor the socket.
func TestNoAcknowledgementUnlessAsked(t *testing.T) {
	f := newFixture(t)
	target := filepath.Join(t.TempDir(), "mount")
	if _, err := f.driver.NodePublishVolume(context.Background(), publishRequest("csi-plain", target, twoSources)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"..chandler.json", "..chandler.sock"} {
		if _, err := os.Lstat(filepath.Join(f.mounts.mounts[target], name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s exists in a volume without appAck: %v", name, err)
		}
	}
}

// tokenSent is what a real client does when it sends a Subscribe: it asks the
// handler for the token at that moment.
func tokenSent(f fixture, id string) string {
	return f.server.subs[id].Handler.Token()
}

// The pod's token goes to the server with every subscription, and one that
// kubelet supplies later repairs a subscription the server refused for want
// of it.
func TestPodTokenIsPassedAndRenewed(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mount")
	req := publishRequest("csi-token", target, "- uri: file:///app.yaml\n  path: app.yaml\n")
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	// Only the token for the server's audience.
	if got := tokenSent(f, "csi-token/0"); got != "token-1" {
		t.Fatalf("subscribed with token %q, want token-1", got)
	}

	// kubelet publishes again with a renewed token: nothing is remounted or
	// resubscribed, as the server has refused nothing.
	f.server.subs = map[string]client.Subscription{}
	req.VolumeContext[tokensKey] = `{"chandler":{"token":"token-2"}}`
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Errorf("a renewed token caused %d subscriptions; the server did not ask for one", n)
	}

	refuse := func(sub client.Subscription) {
		sub.Handler.Fail(&rpcstatus.Status{Code: int32(codes.Unauthenticated), Message: "no token"})
	}
	restartAndSubscribeWithoutToken := func() (*Driver, client.Subscription) {
		t.Helper()
		restarted := f.restart(t)
		if err := restarted.Restore(); err != nil {
			t.Fatal(err)
		}
		restored := f.server.subs["csi-token/0"]
		// The subscription goes out: with no token, as tokens are not kept
		// on disk.
		if got := restored.Handler.Token(); got != "" {
			t.Fatalf("the restored volume subscribed with token %q; tokens must not be kept on disk", got)
		}
		f.server.subs = map[string]client.Subscription{}
		return restarted, restored
	}

	// After a driver restart: the server refuses, then kubelet supplies the
	// token, and the volume subscribes again with it.
	restarted, restored := restartAndSubscribeWithoutToken()
	refuse(restored)
	if _, err := restarted.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.server.subs["csi-token/0"]; !ok || tokenSent(f, "csi-token/0") != "token-2" {
		t.Errorf("after kubelet supplied a token the volume was not subscribed again with it")
	}

	// The other order: kubelet supplies the token after the subscription
	// went out but before the refusal comes back. The refusal is about the
	// subscription without a token, so the volume must still subscribe again.
	restarted, restored = restartAndSubscribeWithoutToken()
	if _, err := restarted.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Fatalf("subscribed again before the server refused anything")
	}
	refuse(restored)
	if _, err := restarted.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.server.subs["csi-token/0"]; !ok || tokenSent(f, "csi-token/0") != "token-2" {
		t.Errorf("a token that arrived before the refusal was never used: the volume stays refused")
	}
	// Once subscribed with the token, a refusal is not answered by
	// subscribing with the same token over and over.
	f.server.subs = map[string]client.Subscription{}
	if _, err := restarted.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Errorf("resubscribed %d times with a token that was already sent", n)
	}

	// The files were there all along.
	if got := read(t, filepath.Join(f.mounts.mounts[target], "app.yaml")); got != "a: 1\n" {
		t.Errorf("app.yaml = %q", got)
	}
}

func noStore(u *chandlerv1.ConfigUpdate) *chandlerv1.ConfigUpdate {
	u.SetCacheControl(chandlerv1.CacheControl_builder{NoStore: proto.Bool(true)}.Build())
	return u
}

// Content a provider marks no_store never reaches the node's disk: it is
// written only to a volume kept in memory, and refused, with the way out, to
// one that is not.
func TestNoStoreNeedsAVolumeInMemory(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	const sources = "- uri: file:///app.yaml\n  path: app.yaml\n"

	target := filepath.Join(t.TempDir(), "memory")
	req := publishRequest("csi-memory", target, sources)
	req.VolumeContext["medium"] = "Memory"
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(f.state, "volumes", "csi-memory", "data")
	if size := f.mounts.memory[data]; size == 0 {
		t.Fatalf("the data directory of a memory volume is not an in-memory filesystem (mounts: %v)", f.mounts.mounts)
	}
	sub := f.server.subs["csi-memory/0"]
	secret := noStore(update("csi-memory/0", "secret"))
	if err := sub.Handler.Apply(secret); err != nil {
		t.Fatalf("no_store content on a memory volume: %v", err)
	}
	if got := sub.Handler.Cached(); len(got) != 1 {
		t.Errorf("the node cache holds %d versions; the no_store one must not be among them", len(got))
	}

	// A driver restart finds the volume only while its memory is mounted.
	restarted := f.restart(t)
	if err := restarted.Restore(); err != nil {
		t.Fatal(err)
	}
	if n := f.server.subscribed(); n != 1 {
		t.Fatalf("%d subscriptions after a restart, want the memory volume's", n)
	}
	delete(f.mounts.mounts, data) // as after a reboot
	f.server.subs = map[string]client.Subscription{}
	if err := f.restart(t).Restore(); err != nil {
		t.Fatal(err)
	}
	if n := f.server.subscribed(); n != 0 {
		t.Errorf("a memory volume whose memory is gone was resumed")
	}

	// On a volume on disk the same content is refused, and a mount waiting
	// for it fails at once and says what to do.
	f = newFixture(t)
	f.server.noStore = true
	_, err := f.driver.NodePublishVolume(ctx, publishRequest("csi-disk", filepath.Join(t.TempDir(), "disk"), sources))
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "medium: Memory") {
		t.Errorf("no_store content on a disk volume: got %v, want FailedPrecondition naming the attribute", err)
	}
	if len(f.mounts.mounts) != 0 {
		t.Errorf("mounted despite the refusal: %v", f.mounts.mounts)
	}

	// Unpublishing removes the memory with the volume.
	f = newFixture(t)
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := f.driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-memory", TargetPath: target}); err != nil {
		t.Fatal(err)
	}
	if len(f.mounts.memory) != 0 || len(f.mounts.mounts) != 0 {
		t.Errorf("left behind: memory %v, mounts %v", f.mounts.memory, f.mounts.mounts)
	}
}

// What goes wrong for a pod's configuration is recorded on the pod.
func TestEventsOnThePod(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mount")
	req := publishRequest("csi-events", target, "- name: app\n  uri: file:///app.yaml\n  path: app.yaml\n")
	req.VolumeContext["appAck"] = "true"
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	sub := f.server.subs["csi-events/0"]
	socket := filepath.Join(f.mounts.mounts[target], "..chandler.sock")
	generation := func() string {
		var config struct{ Generation string }
		_, body := call(t, socket, http.MethodGet, "/v1/config", "")
		if err := json.Unmarshal([]byte(body), &config); err != nil {
			t.Fatal(err)
		}
		return config.Generation
	}

	// A version the volume can never take.
	bad := update("csi-events/0", "x")
	bad.GetBundle().GetFiles()[0].SetPath("../escape")
	bad.SetVersion(bundle.Version(bad.GetBundle()))
	if err := sub.Handler.Apply(bad); err == nil {
		t.Fatal("an unsafe bundle was applied")
	}
	if !f.events.has(events.ReasonRefused, `source "app" (file:///app.yaml)`) {
		t.Errorf("no ConfigRefused event naming the source; recorded: %v", f.events.events)
	}

	// A version the server could not confirm.
	stale := update("csi-events/0", "stale")
	stale.SetStale(true)
	if err := sub.Handler.Apply(stale); err != nil {
		t.Fatal(err)
	}
	if !f.events.has(events.ReasonStale, "could not confirm") {
		t.Errorf("no ConfigStale event; recorded: %v", f.events.events)
	}

	// A version the application rejects, with nothing loaded to go back to.
	if code, body := call(t, socket, http.MethodPost, "/v1/reports", `{"generation":"`+generation()+`","state":"STATE_REJECTED","message":"cannot parse"}`); code != http.StatusNoContent {
		t.Fatalf("reporting rejected = %d %s", code, body)
	}
	if !f.events.has(events.ReasonRejected, "cannot parse") || !f.events.has(events.ReasonRejected, "the files stay") {
		t.Errorf("no ConfigRejected event with the application's reason; recorded: %v", f.events.events)
	}
}

// A mount from the node cache is recorded: the pod runs on what may be old.
func TestEventForAMountFromTheCache(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	warm := filepath.Join(t.TempDir(), "warm")
	if _, err := f.driver.NodePublishVolume(ctx, publishRequest("csi-warm", warm, twoSources)); err != nil {
		t.Fatal(err)
	}
	f.server.content = map[string]string{}
	f.server.down = errors.New("config server is unreachable: connection refused")
	req := publishRequest("csi-cached", filepath.Join(t.TempDir(), "cached"), twoSources)
	req.VolumeContext["failurePolicy"] = "UseCached"
	if _, err := f.driver.NodePublishVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if !f.events.has(events.ReasonFromCache, "node's cache") {
		t.Errorf("no ConfigFromCache event; recorded: %v", f.events.events)
	}
}
