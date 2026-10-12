package driver

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeMounter records mounts instead of performing them.
type fakeMounter struct {
	mounts  map[string]string // target -> source
	failing error
}

func (f *fakeMounter) BindReadOnly(source, target string) error {
	if f.failing != nil {
		return f.failing
	}
	f.mounts[target] = source
	return nil
}

func (f *fakeMounter) Unmount(target string) error {
	delete(f.mounts, target)
	return nil
}

func (f *fakeMounter) IsMountPoint(target string) (bool, error) {
	_, ok := f.mounts[target]
	return ok, nil
}

func newDriver(t *testing.T) (*Driver, *fakeMounter, string) {
	t.Helper()
	m := &fakeMounter{mounts: map[string]string{}}
	state := t.TempDir()
	d, err := New(Config{NodeID: "node-1", Version: "test", StateDir: state, Mounter: m})
	if err != nil {
		t.Fatal(err)
	}
	return d, m, state
}

func publishRequest(volumeID, target string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: target,
		Readonly:   true,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY},
		},
		VolumeContext: map[string]string{ephemeralKey: "true"},
	}
}

func TestIdentityAndNodeInfo(t *testing.T) {
	d, _, _ := newDriver(t)
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

func TestPublishAndUnpublish(t *testing.T) {
	d, m, state := newDriver(t)
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "pods", "uid", "mount")
	data := filepath.Join(state, "volumes", "csi-abc", "data")

	if _, err := d.NodePublishVolume(ctx, publishRequest("csi-abc", target)); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if got := m.mounts[target]; got != data {
		t.Errorf("mounted %q at target, want %q", got, data)
	}
	// The volume was written before it was mounted, so a pod never sees a
	// directory the atomic writer has not initialised.
	if _, err := os.Readlink(filepath.Join(data, "..data")); err != nil {
		t.Errorf("volume has no first generation: %v", err)
	}
	generation, _ := os.Readlink(filepath.Join(data, "..data"))

	// Publishing again is a no-op.
	if _, err := d.NodePublishVolume(ctx, publishRequest("csi-abc", target)); err != nil {
		t.Fatalf("second NodePublishVolume: %v", err)
	}
	if again, _ := os.Readlink(filepath.Join(data, "..data")); again != generation {
		t.Errorf("second publish rewrote the volume: %q -> %q", generation, again)
	}

	unpublish := &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-abc", TargetPath: target}
	if _, err := d.NodeUnpublishVolume(ctx, unpublish); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if len(m.mounts) != 0 {
		t.Errorf("mounts left behind: %v", m.mounts)
	}
	for _, path := range []string{target, filepath.Join(state, "volumes", "csi-abc")} {
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s still exists: %v", path, err)
		}
	}

	// Unpublishing again is a no-op.
	if _, err := d.NodeUnpublishVolume(ctx, unpublish); err != nil {
		t.Errorf("second NodeUnpublishVolume: %v", err)
	}
}

func TestPublishRejectsBadRequests(t *testing.T) {
	d, m, state := newDriver(t)
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
	}
	for name, mutate := range tests {
		req := publishRequest("csi-abc", target)
		mutate(req)
		_, err := d.NodePublishVolume(ctx, req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
	if len(m.mounts) != 0 {
		t.Errorf("a rejected request mounted something: %v", m.mounts)
	}
	entries, _ := os.ReadDir(filepath.Join(state, "volumes"))
	if len(entries) != 0 {
		t.Errorf("a rejected request left a volume directory: %v", entries)
	}

	for _, id := range []string{"", "..", "a/b"} {
		_, err := d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: id, TargetPath: target})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("unpublish %q: got %v, want InvalidArgument", id, err)
		}
	}
}

func TestPublishMountFailure(t *testing.T) {
	d, m, _ := newDriver(t)
	m.failing = errors.New("boom")
	_, err := d.NodePublishVolume(context.Background(), publishRequest("csi-abc", filepath.Join(t.TempDir(), "mount")))
	if status.Code(err) != codes.Internal {
		t.Errorf("got %v, want Internal", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	m := &fakeMounter{}
	for name, cfg := range map[string]Config{
		"no node ID":   {StateDir: t.TempDir(), Mounter: m},
		"no state dir": {NodeID: "n", Mounter: m},
		"no mounter":   {NodeID: "n", StateDir: t.TempDir()},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}
