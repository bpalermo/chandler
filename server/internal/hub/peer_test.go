package hub

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

// seed gives a hub one entry for file:///app.yaml through its provider, with
// no subscription left on it.
func seed(t *testing.T, h harness, revision string, b *chandlerv1.Bundle) {
	t.Helper()
	n := h.connect(t)
	n.subscribe(t, "seed/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emit(watch.GetWatchId(), "seed-"+revision, revision, b)
	n.expectUpdate(t, "seed/0", b)
	h.provider.expectAck(t, "seed-"+revision)
	n.unsubscribe(t, "seed/0")
	if req := h.provider.next(t); !req.HasUnwatch() {
		t.Fatalf("provider got %v, want the unwatch", req)
	}
}

func peersOf(targets ...string) Peers {
	return Peers{
		Service:  "peers:9000",
		Interval: 20 * time.Millisecond,
		WarmUp:   5 * time.Second,
		Resolve:  func(context.Context) ([]string, error) { return targets, nil },
	}
}

// runPeers starts a hub's peer sync and returns a channel closed when it
// reports ready.
func runPeers(t *testing.T, h harness, peers Peers) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go h.hub.RunPeers(ctx, peers, func() { close(ready) })
	return ready
}

func awaitReady(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(wait):
		t.Fatal("the replica never reported ready")
	}
}

// A starting replica copies what a peer holds before it reports ready, and
// can then serve it with every provider unreachable. This is what lets the
// cache survive a rolling restart without a persistent volume.
func TestWarmUpFromAPeer(t *testing.T) {
	old := startWith(t, Config{}, newFakeProvider())
	v1 := oneFile("v1")
	seed(t, old, "r1", v1)

	fresh := startWith(t, Config{CacheDir: t.TempDir()}, nil)
	select {
	case <-runPeers(t, fresh, peersOf(old.target)):
	case <-time.After(wait):
		t.Fatal("the replica never reported ready")
	}
	// Ready means warm: the entry is there at the moment of readiness.
	if got := fresh.hub.entryCount(); got != 1 {
		t.Fatalf("%d entries when ready, want the peer's one", got)
	}
	n := fresh.connect(t)
	n.subscribe(t, "a/0", "file:///app.yaml", "")
	if u := n.expectUpdate(t, "a/0", v1); !u.GetStale() || !u.HasBundle() {
		t.Errorf("update = %v, want the copied bundle marked stale", u)
	}
	// The copy went to disk like any other entry.
	if _, err := fresh.hub.store.Get(bundle.Version(v1)); err != nil {
		t.Errorf("the copied bundle is not in the store: %v", err)
	}
}

// With nobody to copy from, a replica is ready at once: the first replica, or
// the whole fleet was lost.
func TestReadyWithoutPeers(t *testing.T) {
	for name, peers := range map[string]Peers{
		"peer sync disabled": {},
		"no ready peer":      peersOf(),
		"unreachable peer":   peersOf("127.0.0.1:1"),
	} {
		t.Run(name, func(t *testing.T) {
			h := startWith(t, Config{}, nil)
			awaitReady(t, runPeers(t, h, peers))
		})
	}
}

// A version one replica learns from the provider reaches the pods subscribed
// to another replica whose own provider stream is down.
func TestAntiEntropyPropagatesANewerVersion(t *testing.T) {
	v1, v2 := oneFile("v1"), oneFile("v2")
	connected := startWith(t, Config{}, newFakeProvider())
	seed(t, connected, "r1", v1)

	cut := startWith(t, Config{}, nil)
	awaitReady(t, runPeers(t, cut, peersOf(connected.target)))
	n := cut.connect(t)
	n.subscribe(t, "a/0", "file:///app.yaml", "")
	n.expectUpdate(t, "a/0", v1)

	seed(t, connected, "r2", v2)
	if u := n.expectUpdate(t, "a/0", v2); !u.GetStale() {
		t.Errorf("update = %v, want v2 marked stale (this replica cannot confirm it)", u)
	}
}

// fakePeer is a PeerSync server that says whatever the test tells it to.
type fakePeer struct {
	chandlerv1.UnimplementedPeerSyncServer
	entries []*chandlerv1.KeyEntry
	bundles map[string]*chandlerv1.Bundle
}

func (f *fakePeer) ListKeys(_ *chandlerv1.ListKeysRequest, stream chandlerv1.PeerSync_ListKeysServer) error {
	for _, e := range f.entries {
		if err := stream.Send(e); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakePeer) GetBundle(_ context.Context, req *chandlerv1.GetBundleRequest) (*chandlerv1.GetBundleResponse, error) {
	return chandlerv1.GetBundleResponse_builder{Bundle: f.bundles[req.GetVersion()]}.Build(), nil
}

func keyEntry(cacheKey, uri string, b *chandlerv1.Bundle, refreshed, referenced time.Time) *chandlerv1.KeyEntry {
	return chandlerv1.KeyEntry_builder{
		Provider:       proto.String("file"),
		CacheKey:       proto.String(cacheKey),
		CurrentVersion: proto.String(bundle.Version(b)),
		Versions: []*chandlerv1.KeyVersion{
			chandlerv1.KeyVersion_builder{Version: proto.String(bundle.Version(b)), Revision: proto.String("peer")}.Build(),
		},
		Aliases:        []*chandlerv1.Alias{chandlerv1.Alias_builder{Uri: proto.String(uri)}.Build()},
		LastRefreshed:  timestamppb.New(refreshed),
		LastReferenced: timestamppb.New(referenced),
	}.Build()
}

// What a peer says is merged with care: bytes are verified, the provider's
// own word beats a peer's, an older confirmation loses, and a key every
// replica let go of is not brought back.
func TestAdoptRules(t *testing.T) {
	now := &clock{now: time.Unix(1_700_000_000, 0)}
	h := startWith(t, Config{Now: now.Now, Grace: time.Hour}, newFakeProvider())
	mine := oneFile("mine")

	// This replica holds /live.yaml from a live watch, and /idle.yaml unwatched.
	n := h.connect(t)
	n.subscribe(t, "live/0", "file:///live.yaml", "")
	watch := h.provider.expectWatch(t, "file:///live.yaml")
	h.provider.emitAs(watch.GetWatchId(), "n1", "r1", "/live.yaml", mine, nil)
	n.expectUpdate(t, "live/0", mine)
	h.provider.expectAck(t, "n1")
	n.subscribe(t, "idle/0", "file:///idle.yaml", "")
	watch = h.provider.expectWatch(t, "file:///idle.yaml")
	h.provider.emitAs(watch.GetWatchId(), "n2", "r1", "/idle.yaml", mine, nil)
	n.expectUpdate(t, "idle/0", mine)
	h.provider.expectAck(t, "n2")
	n.unsubscribe(t, "idle/0")
	h.provider.next(t) // the unwatch

	theirs, tampered := oneFile("theirs"), oneFile("tampered")
	later, earlier := now.Now().Add(time.Minute), now.Now().Add(-time.Minute)
	peer := &fakePeer{
		entries: []*chandlerv1.KeyEntry{
			keyEntry("/live.yaml", "file:///live.yaml", theirs, later, later),                                 // we have a live watch
			keyEntry("/idle.yaml", "file:///idle.yaml", theirs, earlier, later),                               // confirmed before ours
			keyEntry("/forgotten.yaml", "file:///forgotten.yaml", theirs, later, now.Now().Add(-2*time.Hour)), // past the grace period
			keyEntry("/new.yaml", "file:///new.yaml", theirs, later, later),                                   // adopted
			chandlerv1.KeyEntry_builder{ // a provider this replica does not have
				Provider: proto.String("s3"), CacheKey: proto.String("bucket/x"),
				Versions:      []*chandlerv1.KeyVersion{chandlerv1.KeyVersion_builder{Version: proto.String(bundle.Version(theirs))}.Build()},
				LastRefreshed: timestamppb.New(later), LastReferenced: timestamppb.New(later),
			}.Build(),
		},
		bundles: map[string]*chandlerv1.Bundle{bundle.Version(theirs): theirs},
	}
	target, _ := serve(t, func(s *grpc.Server) { chandlerv1.RegisterPeerSyncServer(s, peer) })

	sync := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		if err := h.hub.syncWith(ctx, target, nil); err != nil {
			t.Fatal(err)
		}
	}
	sync()
	current := func(cacheKey string) string {
		h.hub.mu.Lock()
		defer h.hub.mu.Unlock()
		if e := h.hub.entries[entryKey{"file", cacheKey}]; e != nil {
			return e.current().Version
		}
		return ""
	}
	for cacheKey, want := range map[string]string{
		"/live.yaml":      bundle.Version(mine),
		"/idle.yaml":      bundle.Version(mine),
		"/forgotten.yaml": "",
		"/new.yaml":       bundle.Version(theirs),
	} {
		if got := current(cacheKey); got != want {
			t.Errorf("after the sync %s is at %q, want %q", cacheKey, got, want)
		}
	}
	if got := len(h.hub.entries); got != 3 {
		t.Errorf("%d entries, want 3: the s3 one belongs to no provider here", got)
	}

	// A peer that sends bytes that are not the version is not believed.
	peer.entries = []*chandlerv1.KeyEntry{keyEntry("/tampered.yaml", "file:///tampered.yaml", theirs, later, later)}
	peer.bundles[bundle.Version(theirs)] = tampered
	sync()
	if got := current("/tampered.yaml"); got != "" {
		t.Errorf("a bundle that does not match its version was adopted: %q", got)
	}

	// The adopted entry serves a subscription, and its alias came with it.
	n.subscribe(t, "new/0", "file:///new.yaml", "")
	again := h.provider.expectWatch(t, "file:///new.yaml")
	if again.GetKnownRevision() != "peer" {
		t.Errorf("watched the adopted entry with known_revision %q, want the peer's", again.GetKnownRevision())
	}
	h.provider.fail(again.GetWatchId(), codes.Unavailable)
	if u := n.expectUpdate(t, "new/0", theirs); !u.GetStale() {
		t.Errorf("update = %v, want the adopted bundle marked stale", u)
	}
}
