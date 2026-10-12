// Package driver implements the CSI Identity and Node services of the Chandler
// driver. It serves ephemeral inline volumes only: there is no controller, no
// attach and no staging.
//
// A volume is published by subscribing each of its sources to the config
// server, waiting until all of them are written and only then mounting, so a
// container never starts with part of its configuration.
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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/csi/internal/appack"
	"github.com/bpalermo/chandler/csi/internal/client"
	"github.com/bpalermo/chandler/csi/internal/mount"
	"github.com/bpalermo/chandler/csi/internal/volume"
)

const (
	// Name is the CSI driver name pods refer to. The domain is a placeholder
	// (see docs/design.md).
	Name = "csi.chandler.dev"

	// ephemeralKey is set by kubelet in the volume context of inline volumes.
	ephemeralKey = "csi.storage.k8s.io/ephemeral"

	// Set by kubelet in the volume context because the CSIDriver object has
	// podInfoOnMount: true.
	podNameKey        = "csi.storage.k8s.io/pod.name"
	podNamespaceKey   = "csi.storage.k8s.io/pod.namespace"
	podUIDKey         = "csi.storage.k8s.io/pod.uid"
	serviceAccountKey = "csi.storage.k8s.io/serviceAccount.name"

	volumesDir = "volumes"

	dirMode = 0o755

	// DefaultPublishTimeout is how long a mount waits for every source's
	// initial answer. It sits inside kubelet's own two-minute timeout, so
	// the pod's event carries this driver's explanation rather than a bare
	// deadline.
	DefaultPublishTimeout = 90 * time.Second

	// DefaultCachedAfter is how long a volume that may use the node cache
	// waits for an unreachable server first. Long enough for a driver that
	// has just started to connect; short enough that an outage does not
	// hold pods back.
	DefaultCachedAfter = 5 * time.Second
)

// Subscriber is the driver's side of the stream to the config server.
type Subscriber interface {
	// Subscribe registers a subscription, now and on every reconnect.
	Subscribe(client.Subscription)
	// Unsubscribe ends a subscription.
	Unsubscribe(id string)
	// Report relays what an application said about a version.
	Report(*chandlerv1.AppReport)
	// Err says why the server is unreachable, or nil when it is not.
	Err() error
}

// Driver serves the CSI Identity and Node services.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID      string
	version     string
	stateDir    string
	mounter     mount.Mounter
	server      Subscriber
	cache       volume.Cache
	timeout     time.Duration
	cachedAfter time.Duration

	// mu guards published.
	mu sync.Mutex
	// published is the volumes that are subscribed, by volume ID.
	published map[string]*volume.Volume
	// apis is the application acknowledgement API of the volumes that have
	// one, by volume ID.
	apis map[string]*appack.Server

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
	// Server delivers the configuration.
	Server Subscriber
	// Cache is the node cache.
	Cache volume.Cache
	// PublishTimeout bounds the wait for a volume's sources; zero means
	// DefaultPublishTimeout.
	PublishTimeout time.Duration
	// CachedAfter is how long a volume with failurePolicy UseCached waits
	// for an unreachable server before it is mounted from the node cache;
	// zero means DefaultCachedAfter.
	CachedAfter time.Duration
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
	case cfg.Server == nil:
		return nil, errors.New("config server client is required")
	case cfg.Cache == nil:
		return nil, errors.New("node cache is required")
	}
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = DefaultPublishTimeout
	}
	if cfg.CachedAfter == 0 {
		cfg.CachedAfter = DefaultCachedAfter
	}
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, volumesDir), dirMode); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	return &Driver{
		nodeID:      cfg.NodeID,
		version:     cfg.Version,
		stateDir:    cfg.StateDir,
		mounter:     cfg.Mounter,
		server:      cfg.Server,
		cache:       cfg.Cache,
		timeout:     cfg.PublishTimeout,
		cachedAfter: cfg.CachedAfter,
		published:   make(map[string]*volume.Volume),
		apis:        make(map[string]*appack.Server),
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

// NodePublishVolume implements csi.NodeServer: it subscribes the volume's
// sources, waits until every one of them is written and bind-mounts the
// result read-only at the target path. It is idempotent.
//
// A failure leaves nothing behind, so kubelet's retry starts again from the
// beginning.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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
	attributes := req.GetVolumeContext()
	spec, err := volume.ParseSpec(attributes)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
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

	// Whatever an earlier attempt left is discarded: a volume is mounted
	// complete or not at all.
	d.discard(volumeID)
	v, err := volume.Create(d.volumePath(volumeID), volume.Meta{
		VolumeID:   volumeID,
		TargetPath: target,
		Workload: volume.Workload{
			Namespace:      attributes[podNamespaceKey],
			PodName:        attributes[podNameKey],
			PodUID:         attributes[podUIDKey],
			ServiceAccount: attributes[serviceAccountKey],
		},
		Spec: spec,
	}, d.cache)
	if err != nil {
		d.discard(volumeID)
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	d.subscribe(v)

	if err := d.waitReady(ctx, v, spec.FailurePolicy); err != nil {
		d.discard(volumeID)
		// When the server cannot be reached, that is the cause worth
		// showing in the pod's event.
		if serverErr := d.server.Err(); serverErr != nil && status.Code(err) == codes.Unavailable {
			return nil, status.Errorf(codes.Unavailable, "%s: %v", status.Convert(err).Message(), serverErr)
		}
		return nil, err
	}

	// The socket is there before the mount, so the application finds it
	// from its first instruction.
	if err := d.serveAppAck(v); err != nil {
		d.discard(volumeID)
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if err := os.MkdirAll(target, dirMode); err != nil {
		d.discard(volumeID)
		return nil, status.Errorf(codes.Internal, "create target path: %v", err)
	}
	if err := d.mounter.BindReadOnly(v.DataDir(), target); err != nil {
		d.discard(volumeID)
		return nil, status.Errorf(codes.Internal, "mount volume: %v", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume implements csi.NodeServer: it ends the volume's
// subscriptions, unmounts the target path and removes the volume's directory.
// It is idempotent.
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
	if err := d.discard(volumeID); err != nil {
		return nil, status.Errorf(codes.Internal, "remove volume directory: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// Restore resumes the volumes a previous run of the driver left mounted, so
// they keep receiving updates across a driver restart. Each is subscribed
// again with the versions it already has.
//
// A volume directory whose target is no longer mounted (the node rebooted, or
// a publish was interrupted) is removed: kubelet publishes the volume again if
// its pod still exists.
func (d *Driver) Restore() error {
	entries, err := os.ReadDir(filepath.Join(d.stateDir, volumesDir))
	if err != nil {
		return fmt.Errorf("read state directory: %w", err)
	}
	for _, entry := range entries {
		volumeID := entry.Name()
		if !entry.IsDir() || !validVolumeID(volumeID) {
			continue
		}
		unlock := d.volumes.lock(volumeID)
		v, err := volume.Load(d.volumePath(volumeID), d.cache)
		mounted := false
		if err == nil {
			mounted, err = d.mounter.IsMountPoint(v.Meta().TargetPath)
		}
		if err != nil || !mounted {
			if err := d.discard(volumeID); err != nil {
				unlock()
				return fmt.Errorf("remove stale volume %s: %w", volumeID, err)
			}
		} else {
			d.subscribe(v)
			// The socket's listener died with the last driver.
			if err := d.serveAppAck(v); err != nil {
				unlock()
				return fmt.Errorf("volume %s: %w", volumeID, err)
			}
		}
		unlock()
	}
	return nil
}

// waitReady waits until every source of v is written.
//
// With failurePolicy UseCached, a source the server cannot supply is written
// from the node cache instead: when the server has been unreachable for a
// short while, when it says it has no version because its provider is down,
// or when the wait runs out. The subscriptions stay, so the volume converges
// on the server's version once it answers.
func (d *Driver) waitReady(ctx context.Context, v *volume.Volume, policy string) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	if policy != volume.FailurePolicyUseCached {
		return v.WaitReady(ctx)
	}

	started := time.Now()
	for {
		step, stop := context.WithTimeout(ctx, time.Second)
		err := v.WaitReady(step)
		stop()
		if err == nil || status.Code(err) != codes.Unavailable {
			return err
		}
		unreachable := d.server.Err() != nil && time.Since(started) >= d.cachedAfter
		refused := v.Failure() != nil
		if unreachable || refused || ctx.Err() != nil {
			if cacheErr := v.ApplyCached(); cacheErr != nil {
				return cacheErr
			}
			return nil
		}
	}
}

// InUse returns the canonical URIs of the volumes on this node, which the
// node cache must keep.
func (d *Driver) InUse() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var uris []string
	for _, v := range d.published {
		uris = append(uris, v.URIs()...)
	}
	return uris
}

// subscribe registers every source of v with the config server.
func (d *Driver) subscribe(v *volume.Volume) {
	meta := v.Meta()
	d.mu.Lock()
	d.published[meta.VolumeID] = v
	d.mu.Unlock()

	workload := chandlerv1.Workload_builder{
		Namespace:      proto.String(meta.Workload.Namespace),
		PodName:        proto.String(meta.Workload.PodName),
		PodUid:         proto.String(meta.Workload.PodUID),
		ServiceAccount: proto.String(meta.Workload.ServiceAccount),
	}.Build()
	for i, source := range meta.Spec.Sources {
		d.server.Subscribe(client.Subscription{
			ID:       v.SubscriptionID(i),
			URI:      source.URI,
			Workload: workload,
			AppAck:   v.AppAck(),
			Handler:  sourceHandler{volume: v, index: i},
		})
	}
}

// serveAppAck starts the application acknowledgement API of a volume that
// asked for one, forwarding every verdict to the config server.
func (d *Driver) serveAppAck(v *volume.Volume) error {
	if !v.AppAck() {
		return nil
	}
	api, err := appack.Serve(v.DataDir(), v, func(outcomes []volume.Outcome) {
		for _, o := range outcomes {
			state := chandlerv1.AppReport_STATE_LOADED
			if o.State == volume.StateRejected {
				state = chandlerv1.AppReport_STATE_REJECTED
			}
			report := chandlerv1.AppReport_builder{
				SubscriptionId: proto.String(v.SubscriptionID(o.Source)),
				Version:        proto.String(o.Version),
				State:          state.Enum(),
			}
			if o.Message != "" {
				report.Message = proto.String(o.Message)
			}
			if o.Applied != "" {
				report.AppliedVersion = proto.String(o.Applied)
			}
			d.server.Report(report.Build())
		}
	})
	if err != nil {
		return fmt.Errorf("serve the application acknowledgement socket: %w", err)
	}
	d.mu.Lock()
	d.apis[v.Meta().VolumeID] = api
	d.mu.Unlock()
	return nil
}

// discard ends the subscriptions of a volume, if it has any, and removes its
// directory. The caller holds the volume's lock and has unmounted it.
func (d *Driver) discard(volumeID string) error {
	d.mu.Lock()
	v := d.published[volumeID]
	delete(d.published, volumeID)
	api := d.apis[volumeID]
	delete(d.apis, volumeID)
	d.mu.Unlock()
	if api != nil {
		api.Close()
	}
	if v != nil {
		for i := range v.Meta().Spec.Sources {
			d.server.Unsubscribe(v.SubscriptionID(i))
		}
		v.Close()
	}
	return os.RemoveAll(d.volumePath(volumeID))
}

// sourceHandler is one source of a volume, as the client's handler.
type sourceHandler struct {
	volume *volume.Volume
	index  int
}

func (h sourceHandler) Apply(update *chandlerv1.ConfigUpdate) error {
	return h.volume.Apply(h.index, update)
}
func (h sourceHandler) Fail(st *rpcstatus.Status) { h.volume.Fail(h.index, st) }
func (h sourceHandler) Applied() string           { return h.volume.Applied(h.index) }
func (h sourceHandler) Cached() []string          { return h.volume.CachedVersions(h.index) }
func (h sourceHandler) Loaded() string            { return h.volume.Loaded(h.index) }

func (d *Driver) volumePath(volumeID string) string {
	return filepath.Join(d.stateDir, volumesDir, volumeID)
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
