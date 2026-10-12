package hub

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/rollout"
)

const half = 5000

func (n *node) subscribePod(t *testing.T, id, uri, podUID string) {
	t.Helper()
	n.send(t, chandlerv1.ConfigRequest_builder{
		Subscribe: chandlerv1.Subscribe_builder{
			SubscriptionId: proto.String(id),
			Uri:            proto.String(uri),
			Workload:       chandlerv1.Workload_builder{PodUid: proto.String(podUID)}.Build(),
		}.Build(),
	}.Build())
}

func (p *fakeProvider) emitShare(watchID, nonce, revision string, b *chandlerv1.Bundle, basisPoints uint32) {
	p.responses <- chandlerv1.ProviderResponse_builder{
		Update: chandlerv1.ResourceUpdate_builder{
			WatchId:  proto.String(watchID),
			Nonce:    proto.String(nonce),
			CacheKey: proto.String("/app.yaml"),
			Revision: proto.String(revision),
			Bundle:   b,
			Rollout:  chandlerv1.Rollout_builder{BasisPoints: proto.Uint32(basisPoints)}.Build(),
		}.Build(),
	}.Build()
}

// advance moves a revision's rollout on without sending its bytes; a share of
// rollout.Everyone releases it.
func (p *fakeProvider) advance(watchID, nonce, revision string, basisPoints uint32) {
	u := chandlerv1.ResourceUpdate_builder{
		WatchId:     proto.String(watchID),
		Nonce:       proto.String(nonce),
		CacheKey:    proto.String("/app.yaml"),
		Revision:    proto.String(revision),
		NotModified: &chandlerv1.NotModified{},
	}
	if basisPoints < rollout.Everyone {
		u.Rollout = chandlerv1.Rollout_builder{BasisPoints: proto.Uint32(basisPoints)}.Build()
	}
	p.responses <- chandlerv1.ProviderResponse_builder{Update: u.Build()}.Build()
}

// pods returns want pod UIDs on each side of a rollout of b at the share.
func pods(b *chandlerv1.Bundle, basisPoints uint32, want int) (in, out []string) {
	for i := 0; len(in) < want || len(out) < want; i++ {
		uid := fmt.Sprintf("pod-%d", i)
		if rollout.Placed(bundle.Version(b), uid, basisPoints) {
			if len(in) < want {
				in = append(in, uid)
			}
		} else if len(out) < want {
			out = append(out, uid)
		}
	}
	return in, out
}

// rolloutFixture is a hub whose /app.yaml is at v1 for four pods, two of
// which a rollout of v2 at half the fleet selects.
type rolloutFixture struct {
	harness
	node    *node
	watch   string
	v1, v2  *chandlerv1.Bundle
	in, out []string
}

func startRollout(t *testing.T, cfg Config) rolloutFixture {
	t.Helper()
	f := rolloutFixture{harness: startWith(t, cfg, newFakeProvider()), v1: oneFile("v1"), v2: oneFile("v2")}
	f.in, f.out = pods(f.v2, half, 2)
	f.node = f.connect(t)
	for i, uid := range append(append([]string{}, f.in...), f.out...) {
		f.node.subscribePod(t, uid+"/0", "file:///app.yaml", uid)
		if i == 0 {
			f.watch = f.provider.expectWatch(t, "file:///app.yaml").GetWatchId()
			f.provider.emit(f.watch, "n1", "r1", f.v1)
		}
		f.node.send(t, ack(f.node.expectUpdate(t, uid+"/0", f.v1)))
	}
	f.provider.expectAck(t, "n1")
	return f
}

func ids(uids []string) []string {
	out := make([]string, len(uids))
	for i, uid := range uids {
		out[i] = uid + "/0"
	}
	return out
}

// The point of a rollout: a new version reaches the share of pods the provider
// names, and the rest only when the provider says so.
func TestRolloutReachesAShareThenEveryone(t *testing.T) {
	f := startRollout(t, Config{})

	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	for range f.in {
		u := f.node.next(t).GetUpdate()
		if u.GetVersion() != bundle.Version(f.v2) || u.GetTrack() != chandlerv1.Track_TRACK_CANDIDATE || u.GetRollout() != bundle.Version(f.v2) {
			t.Fatalf("a selected pod got %v, want v2 on the candidate track", u)
		}
		if u.GetSubscriptionId() != f.in[0]+"/0" && u.GetSubscriptionId() != f.in[1]+"/0" {
			t.Fatalf("%s got the candidate but is not in the share", u.GetSubscriptionId())
		}
	}
	f.node.expectNothing(t) // the rest keep v1

	// A pod that starts mid-rollout is placed by the same rule.
	lateIn, lateOut := pods(f.v2, half, 3)
	f.node.subscribePod(t, "late-in/0", "file:///app.yaml", lateIn[2])
	f.node.expectUpdate(t, "late-in/0", f.v2)
	f.node.subscribePod(t, "late-out/0", "file:///app.yaml", lateOut[2])
	if u := f.node.expectUpdate(t, "late-out/0", f.v1); u.GetTrack() != chandlerv1.Track_TRACK_STABLE || u.GetRollout() != bundle.Version(f.v2) {
		t.Errorf("a pod outside the share got %v, want v1 on the stable track of the v2 rollout", u)
	}

	// The share never shrinks: a lower one is ignored.
	f.provider.advance(f.watch, "n3", "r2", 1)
	f.node.expectNothing(t)

	// Released: everyone else gets it, and nothing says "rollout" any more.
	f.provider.advance(f.watch, "n4", "r2", rollout.Everyone)
	f.node.expectUpdates(t, f.v2, append(ids(f.out), "late-out/0")...)
	f.node.subscribePod(t, "after/0", "file:///app.yaml", "after")
	if u := f.node.expectUpdate(t, "after/0", f.v2); u.GetTrack() != chandlerv1.Track_TRACK_UNSPECIFIED || u.HasRollout() {
		t.Errorf("after the rollout a pod got %v, want v2 with no rollout", u)
	}
}

// To take a version back, the provider emits the previous content again. Pods
// that had the candidate return to it, with no bytes: their node has it.
func TestRolloutTakenBack(t *testing.T) {
	f := startRollout(t, Config{})
	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	for range f.in {
		f.node.send(t, ack(f.node.next(t).GetUpdate()))
	}

	f.provider.emit(f.watch, "n3", "r3", f.v1)
	for range f.in {
		u := f.node.next(t).GetUpdate()
		if u.GetVersion() != bundle.Version(f.v1) || !u.HasUseCached() || u.HasRollout() {
			t.Fatalf("a pod on the withdrawn candidate got %v, want v1 as UseCached with no rollout", u)
		}
	}
	f.node.expectNothing(t) // the pods that stayed on v1 hear nothing
}

// A newer version replaces a rollout in progress: the stable version stays
// what everyone had, and placement is by the new version.
func TestRolloutSuperseded(t *testing.T) {
	f := startRollout(t, Config{})
	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	for range f.in {
		f.node.next(t)
	}

	v3 := oneFile("v3")
	f.provider.emitShare(f.watch, "n3", "r3", v3, half)
	all := append(append([]string{}, f.in...), f.out...)
	seen := map[string]string{}
	for {
		select {
		case msg := <-f.node.in:
			seen[msg.GetUpdate().GetSubscriptionId()] = msg.GetUpdate().GetVersion()
			continue
		case <-time.After(300 * time.Millisecond):
		}
		break
	}
	for _, uid := range all {
		was := bundle.Version(f.v1)
		if rollout.Placed(bundle.Version(f.v2), uid, half) {
			was = bundle.Version(f.v2)
		}
		want := bundle.Version(f.v1)
		if rollout.Placed(bundle.Version(v3), uid, half) {
			want = bundle.Version(v3)
		}
		got, moved := seen[uid+"/0"]
		if !moved {
			got = was
		}
		if got != want {
			t.Errorf("%s ended on %s, want %s", uid, got, want)
		}
	}
}

// A resource's first version goes to everyone whatever the share: there is
// nothing else to give the rest.
func TestFirstVersionIsNotRolledOut(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	n := h.connect(t)
	v1 := oneFile("v1")
	_, out := pods(v1, 100, 1)
	n.subscribePod(t, "a/0", "file:///app.yaml", out[0])
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emitShare(watch.GetWatchId(), "n1", "r1", v1, 100)
	if u := n.expectUpdate(t, "a/0", v1); u.HasRollout() {
		t.Errorf("the first version was delivered as a rollout: %v", u)
	}
}

// The provider is told how a revision fares, so it can hold a rollout.
func TestFeedback(t *testing.T) {
	f := startRollout(t, Config{})
	expect := func(revision string, assigned, written, refused uint32) {
		t.Helper()
		deadline := time.After(wait)
		var last *chandlerv1.Feedback
		for {
			select {
			case fb := <-f.provider.feedback:
				last = fb
				if fb.GetRevision() == revision && fb.GetAssigned() == assigned && fb.GetWritten() == written && fb.GetRefused() == refused {
					return
				}
			case <-deadline:
				t.Fatalf("never told %s assigned=%d written=%d refused=%d; last feedback: %v", revision, assigned, written, refused, last)
			}
		}
	}
	expect("r1", 4, 4, 0)

	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	first, second := f.node.next(t).GetUpdate(), f.node.next(t).GetUpdate()
	expect("r2", 2, 0, 0)
	f.node.send(t, ack(first))
	expect("r2", 2, 1, 0)
	// A bundle the volume can never take is what a provider holds on.
	f.node.send(t, nack(second, codes.FailedPrecondition))
	expect("r2", 2, 1, 1)
}

// A rollout in progress survives a restart, share included.
func TestRolloutSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	f := startRollout(t, Config{CacheDir: dir})
	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	for range f.in {
		f.node.next(t)
	}
	if req := f.provider.next(t); !req.HasAck() {
		t.Fatalf("provider got %v, want the ack of the candidate", req)
	}
	f.stop()

	second := startWith(t, Config{CacheDir: dir}, nil)
	n := second.connect(t)
	n.subscribePod(t, "in/0", "file:///app.yaml", f.in[0])
	n.expectUpdate(t, "in/0", f.v2)
	n.subscribePod(t, "out/0", "file:///app.yaml", f.out[0])
	n.expectUpdate(t, "out/0", f.v1)
}

// A replica that copies a rollout from a peer places pods the same way.
func TestRolloutIsCopiedByPeers(t *testing.T) {
	f := startRollout(t, Config{})
	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	for range f.in {
		f.node.next(t)
	}
	if req := f.provider.next(t); !req.HasAck() {
		t.Fatalf("provider got %v, want the ack of the candidate", req)
	}

	other := startWith(t, Config{}, nil)
	awaitReady(t, runPeers(t, other, peersOf(f.target)))
	n := other.connect(t)
	n.subscribePod(t, "in/0", "file:///app.yaml", f.in[0])
	n.expectUpdate(t, "in/0", f.v2)
	n.subscribePod(t, "out/0", "file:///app.yaml", f.out[0])
	n.expectUpdate(t, "out/0", f.v1)
}

// What an application says about a version is carried to the provider, and a
// version it rejected is not sent to it again.
func TestApplicationReports(t *testing.T) {
	f := startRollout(t, Config{})
	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	first, second := f.node.next(t).GetUpdate(), f.node.next(t).GetUpdate()
	report := func(u *chandlerv1.ConfigUpdate, state chandlerv1.AppReport_State, applied string) {
		t.Helper()
		f.node.send(t, ack(u))
		f.node.send(t, chandlerv1.ConfigRequest_builder{
			AppReport: chandlerv1.AppReport_builder{
				SubscriptionId: proto.String(u.GetSubscriptionId()),
				Version:        proto.String(u.GetVersion()),
				State:          state.Enum(),
				AppliedVersion: proto.String(applied),
			}.Build(),
		}.Build())
	}
	report(first, chandlerv1.AppReport_STATE_LOADED, first.GetVersion())
	// The second application refuses it; its driver put v1 back.
	report(second, chandlerv1.AppReport_STATE_REJECTED, bundle.Version(f.v1))

	deadline := time.After(wait)
	for {
		var fb *chandlerv1.Feedback
		select {
		case fb = <-f.provider.feedback:
		case <-deadline:
			t.Fatal("the provider was never told loaded=1 rejected=1")
		}
		if fb.GetRevision() == "r2" && fb.GetLoaded() == 1 && fb.GetRejected() == 1 {
			break
		}
	}

	// Releasing the version to everyone reaches the pods that did not have
	// it, and not the one whose application refused it.
	f.provider.advance(f.watch, "n3", "r2", rollout.Everyone)
	f.node.expectUpdates(t, f.v2, ids(f.out)...)
	f.node.expectNothing(t)

	// The next version is sent to it like to anyone else.
	v3 := oneFile("v3")
	f.provider.emit(f.watch, "n4", "r3", v3)
	f.node.expectUpdates(t, v3, append(ids(f.in), ids(f.out)...)...)
}
