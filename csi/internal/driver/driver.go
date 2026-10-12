// Package driver implements the CSI Identity and Node services of the Chandler
// driver. It serves ephemeral inline volumes only: there is no controller, no
// attach and no staging.
package driver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bpalermo/chandler/csi/internal/atomic"
	"github.com/bpalermo/chandler/csi/internal/mount"
)

const (
	// Name is the CSI driver name pods refer to. The domain is a placeholder
	// (see docs/design.md).
	Name = "csi.chandler.dev"

	// ephemeralKey is set by kubelet in the volume context of inline volumes.
	ephemeralKey = "csi.storage.k8s.io/ephemeral"

	volumesDir = "volumes"
	// dataDir is the directory of a volume that is mounted into the pod.
	dataDir = "data"

	dirMode = 0o755
)

// Driver serves the CSI Identity and Node services.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID   string
	version  string
	stateDir string
	mounter  mount.Mounter

	// volumes serialises operations per volume ID, as the CSI spec lets a
	// plugin assume at most one call in flight per volume.
	volumes keyedMutex
}

// Config configures a Driver.
type Config struct {
	// NodeID is the Kubernetes node name.
	NodeID string
	// Version is reported to kubelet as the plugin's vendor version.
	Version string
	// StateDir is the driver's directory on the host, where volumes live.
	StateDir string
	// Mounter performs the mounts.
	Mounter mount.Mounter
}

// New returns a Driver, creating its state directory if needed.
func New(cfg Config) (*Driver, error) {
	switch {
	case cfg.NodeID == "":
		return nil, errors.New("node ID is required")
	case cfg.StateDir == "":
		return nil, errors.New("state directory is required")
	case cfg.Mounter == nil:
		return nil, errors.New("mounter is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, volumesDir), dirMode); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	return &Driver{
		nodeID:   cfg.NodeID,
		version:  cfg.Version,
		stateDir: cfg.StateDir,
		mounter:  cfg.Mounter,
	}, nil
}

// GetPluginInfo implements csi.IdentityServer.
func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: Name, VendorVersion: d.version}, nil
}

// GetPluginCapabilities implements csi.IdentityServer. The driver has no
// controller service, so it advertises no capabilities.
func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

// Probe implements csi.IdentityServer.
func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

// NodeGetInfo implements csi.NodeServer.
func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.nodeID}, nil
}

// NodeGetCapabilities implements csi.NodeServer. Volumes are published
// directly; there is no staging step.
func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodePublishVolume implements csi.NodeServer: it creates the volume's
// directory, writes its first generation and bind-mounts it read-only at the
// target path. It is idempotent.
//
// Sources are not delivered yet: the volume is mounted with an empty first
// generation. Subscribing to the config server comes with the server.
func (d *Driver) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID, target := req.GetVolumeId(), req.GetTargetPath()
	switch {
	case !validVolumeID(volumeID):
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q", volumeID)
	case target == "":
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	case req.GetVolumeCapability() == nil:
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	case req.GetVolumeCapability().GetMount() == nil:
		return nil, status.Error(codes.InvalidArgument, "only mount volumes are supported")
	case req.GetVolumeContext()[ephemeralKey] != "true":
		return nil, status.Error(codes.InvalidArgument, "only ephemeral inline volumes are supported")
	case !req.GetReadonly():
		return nil, status.Error(codes.InvalidArgument, "the volume must be mounted readOnly: true")
	}

	unlock := d.volumes.lock(volumeID)
	defer unlock()

	mounted, err := d.mounter.IsMountPoint(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check target path: %v", err)
	}
	if mounted {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	data := d.dataPath(volumeID)
	if err := os.MkdirAll(data, dirMode); err != nil {
		return nil, status.Errorf(codes.Internal, "create volume directory: %v", err)
	}
	writer := atomic.NewWriter(data)
	current, err := writer.Current()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read volume: %v", err)
	}
	if current == "" {
		if err := writer.Write(nil); err != nil {
			return nil, status.Errorf(codes.Internal, "write volume: %v", err)
		}
	}

	if err := os.MkdirAll(target, dirMode); err != nil {
		return nil, status.Errorf(codes.Internal, "create target path: %v", err)
	}
	if err := d.mounter.BindReadOnly(data, target); err != nil {
		return nil, status.Errorf(codes.Internal, "mount volume: %v", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume implements csi.NodeServer: it unmounts the target path
// and removes the volume's directory. It is idempotent.
func (d *Driver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID, target := req.GetVolumeId(), req.GetTargetPath()
	switch {
	case !validVolumeID(volumeID):
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q", volumeID)
	case target == "":
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	unlock := d.volumes.lock(volumeID)
	defer unlock()

	if err := d.mounter.Unmount(target); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	// Remove, not RemoveAll: if the unmount silently did nothing, the target
	// still shows the volume and must not be emptied through the mount.
	if err := os.Remove(target); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "remove target path: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(d.stateDir, volumesDir, volumeID)); err != nil {
		return nil, status.Errorf(codes.Internal, "remove volume directory: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *Driver) dataPath(volumeID string) string {
	return filepath.Join(d.stateDir, volumesDir, volumeID, dataDir)
}

// validVolumeID reports whether id is safe to use as a directory name. Kubelet
// generates the IDs of inline volumes, but the driver never trusts a request
// to stay inside its state directory.
func validVolumeID(id string) bool {
	return id != "" && id != "." && id != ".." && len(id) <= 255 &&
		!strings.ContainsAny(id, "/\x00")
}

// keyedMutex is a mutex per key.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// lock locks key and returns the function that unlocks it.
func (k *keyedMutex) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLock)
	}
	l := k.locks[key]
	if l == nil {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
