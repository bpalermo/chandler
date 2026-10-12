package hub

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

func (p *fakeProvider) emitAs(watchID, nonce, revision, key string, b *chandlerv1.Bundle, cc *chandlerv1.CacheControl) {
	p.responses <- chandlerv1.ProviderResponse_builder{
		Update: chandlerv1.ResourceUpdate_builder{
			WatchId:      proto.String(watchID),
			Nonce:        proto.String(nonce),
			CacheKey:     proto.String(key),
			Revision:     proto.String(revision),
			Bundle:       b,
			CacheControl: cc,
		}.Build(),
	}.Build()
}

func (p *fakeProvider) notModified(watchID, nonce, revision string) {
	p.responses <- chandlerv1.ProviderResponse_builder{
		Update: chandlerv1.ResourceUpdate_builder{
			WatchId:     proto.String(watchID),
			Nonce:       proto.String(nonce),
			CacheKey:    proto.String("/app.yaml"),
			Revision:    proto.String(revision),
			NotModified: &chandlerv1.NotModified{},
		}.Build(),
	}.Build()
}

func (p *fakeProvider) fail(watchID string, code codes.Code) {
	p.responses <- chandlerv1.ProviderResponse_builder{
		Error: chandlerv1.WatchError_builder{
			WatchId: proto.String(watchID),
			Status:  &status.Status{Code: int32(code), Message: "from the provider"},
		}.Build(),
	}.Build()
}

// expectAck consumes the provider's next message, which must be an ack.
func (p *fakeProvider) expectAck(t *testing.T, nonce string) {
	t.Helper()
	if req := p.next(t); !req.HasAck() || req.GetAck().GetNonce() != nonce {
		t.Fatalf("provider got %v, want the ack of %s", req, nonce)
	}
}

func (n *node) subscribeAs(t *testing.T, id, uri, namespace string) {
	t.Helper()
	n.send(t, chandlerv1.ConfigRequest_builder{
		Subscribe: chandlerv1.Subscribe_builder{
			SubscriptionId: proto.String(id),
			Uri:            proto.String(uri),
			Workload:       chandlerv1.Workload_builder{Namespace: proto.String(namespace)}.Build(),
		}.Build(),
	}.Build())
}

func (n *node) unsubscribe(t *testing.T, id string) {
	t.Helper()
	n.send(t, chandlerv1.ConfigRequest_builder{
		Unsubscribe: chandlerv1.Unsubscribe_builder{SubscriptionId: proto.String(id)}.Build(),
	}.Build())
}

// An entry outlives its subscriptions: the next pod is answered from it once
// the provider confirms it is current, with no bytes from the provider.
func TestEntryOutlivesItsSubscriptions(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	n := h.connect(t)
	v1 := oneFile("v1")

	n.subscribe(t, "a/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	n.expectUpdate(t, "a/0", v1)
	h.provider.expectAck(t, "n1")
	n.unsubscribe(t, "a/0")
	if req := h.provider.next(t); !req.HasUnwatch() {
		t.Fatalf("provider got %v, want the unwatch", req)
	}

	n.subscribe(t, "b/0", "file:///app.yaml", "")
	again := h.provider.expectWatch(t, "file:///app.yaml")
	if again.GetKnownRevision() != "r1" {
		t.Fatalf("re-watched with known_revision %q, want r1", again.GetKnownRevision())
	}
	n.expectNothing(t) // not served until the provider says it is current
	h.provider.notModified(again.GetWatchId(), "n2", "r1")
	update := n.expectUpdate(t, "b/0", v1)
	if !update.HasBundle() || update.GetStale() || update.GetCacheKey() != "/app.yaml" {
		t.Errorf("update = %v, want the held bundle, fresh, under its cache key", update)
	}
}

// The provider-failure table of the design: stale while the provider is down,
// refused to new subscriptions once expired, and never taken from a pod that
// has it.
func TestStaleAndExpired(t *testing.T) {
	now := &clock{now: time.Unix(1_700_000_000, 0)}
	h := startWith(t, Config{Now: now.Now}, newFakeProvider())
	n := h.connect(t)
	v1 := oneFile("v1")
	cc := chandlerv1.CacheControl_builder{
		MaxAge:       durationpb.New(time.Minute),
		StaleIfError: durationpb.New(time.Hour),
	}.Build()

	n.subscribe(t, "held/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emitAs(watch.GetWatchId(), "n1", "r1", "/app.yaml", v1, cc)
	first := n.expectUpdate(t, "held/0", v1)
	if first.GetStale() || first.GetCacheControl().GetMaxAge().GetSeconds() != 60 {
		t.Errorf("first update = %v, want fresh and carrying the cache control", first)
	}
	h.provider.expectAck(t, "n1")

	h.provider.hangup <- struct{}{}
	other := h.connect(t)
	// Wait until the server has noticed the outage.
	waitFor(t, func() bool {
		h.hub.mu.Lock()
		defer h.hub.mu.Unlock()
		return !h.hub.upstreams["file"].connected()
	})

	other.subscribe(t, "late/0", "file:///app.yaml", "")
	if stale := other.expectUpdate(t, "late/0", v1); !stale.GetStale() || !stale.HasBundle() {
		t.Errorf("during the outage got %v, want the held bundle marked stale", stale)
	}

	// Past max_age + stale_if_error a new subscription gets nothing...
	now.advance(2 * time.Hour)
	other.subscribe(t, "later/0", "file:///app.yaml", "")
	st := other.expectError(t, "later/0", codes.Unavailable)
	if !strings.Contains(st.GetMessage(), "expired") {
		t.Errorf("message = %q, want it to say the version expired", st.GetMessage())
	}
	// ...and the pod that has the version hears nothing: it keeps it.
	n.expectNothing(t)
}

// With no stale_if_error an old version is served for as long as the provider
// is away: for configuration, old is almost always better than none.
func TestStaleIsUnlimitedByDefault(t *testing.T) {
	now := &clock{now: time.Unix(1_700_000_000, 0)}
	h := startWith(t, Config{Now: now.Now}, newFakeProvider())
	n := h.connect(t)
	v1 := oneFile("v1")

	n.subscribe(t, "a/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	n.expectUpdate(t, "a/0", v1)
	h.provider.hangup <- struct{}{}
	waitFor(t, func() bool {
		h.hub.mu.Lock()
		defer h.hub.mu.Unlock()
		return !h.hub.upstreams["file"].connected()
	})
	now.advance(30 * 24 * time.Hour)
	n.subscribe(t, "b/0", "file:///app.yaml", "")
	if u := n.expectUpdate(t, "b/0", v1); !u.GetStale() {
		t.Errorf("update = %v, want it marked stale", u)
	}
}

// The cache directory carries entries across a restart: a new process serves
// what the last one held, before any provider answers.
func TestCacheSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	first := startWith(t, Config{CacheDir: dir}, newFakeProvider())
	n := first.connect(t)
	v1, secret := oneFile("v1"), oneFile("secret")

	n.subscribe(t, "a/0", "file:///app.yaml", "")
	watch := first.provider.expectWatch(t, "file:///app.yaml")
	first.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	n.expectUpdate(t, "a/0", v1)
	first.provider.expectAck(t, "n1")

	// no_store content must not reach the disk, not even by name.
	n.subscribe(t, "s/0", "file:///secret.yaml", "")
	watch = first.provider.expectWatch(t, "file:///secret.yaml")
	first.provider.emitAs(watch.GetWatchId(), "n2", "r1", "/secret.yaml", secret,
		chandlerv1.CacheControl_builder{NoStore: proto.Bool(true)}.Build())
	n.expectUpdate(t, "s/0", secret)
	first.stop()

	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, err := os.ReadFile(p)
		if err == nil && strings.Contains(string(raw), "secret") {
			t.Errorf("no_store content or its name is on disk in %s", p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// The restarted server cannot reach the provider at all.
	second := startWith(t, Config{CacheDir: dir}, nil)
	m := second.connect(t)
	m.subscribe(t, "b/0", "file:///app.yaml", "")
	if u := m.expectUpdate(t, "b/0", v1); !u.GetStale() || !u.HasBundle() {
		t.Errorf("after the restart got %v, want the held bundle marked stale", u)
	}
	m.subscribe(t, "s/1", "file:///secret.yaml", "")
	m.expectError(t, "s/1", codes.Unavailable)
}

func TestQuotas(t *testing.T) {
	h := startWith(t, Config{Quotas: Quotas{
		MaxKeysPerProvider:  1,
		MaxBytesPerProvider: 1 << 20,
		MaxVariantsPerURI:   16,
		MaxURIsPerNamespace: 2,
	}}, newFakeProvider())
	n := h.connect(t)

	// A namespace may not subscribe to more distinct URIs than its quota,
	// however many pods share them.
	n.subscribeAs(t, "a/0", "file:///one.yaml", "shop")
	one := h.provider.expectWatch(t, "file:///one.yaml")
	n.subscribeAs(t, "a/1", "file:///two.yaml", "shop")
	two := h.provider.expectWatch(t, "file:///two.yaml")
	n.subscribeAs(t, "b/0", "file:///one.yaml", "shop")
	n.subscribeAs(t, "c/0", "file:///three.yaml", "shop")
	st := n.expectError(t, "c/0", codes.ResourceExhausted)
	if !strings.Contains(st.GetMessage(), `"shop"`) {
		t.Errorf("message = %q, want it to name the namespace", st.GetMessage())
	}
	// Another namespace has its own quota.
	n.subscribeAs(t, "d/0", "file:///three.yaml", "billing")
	three := h.provider.expectWatch(t, "file:///three.yaml")

	// A provider may not issue more keys than its quota: the bundle is
	// refused to the provider and the subscriber is told.
	h.provider.emitAs(one.GetWatchId(), "n1", "r1", "/one.yaml", oneFile("1"), nil)
	n.expectUpdates(t, oneFile("1"), "a/0", "b/0")
	h.provider.expectAck(t, "n1")
	h.provider.emitAs(two.GetWatchId(), "n2", "r1", "/two.yaml", oneFile("2"), nil)
	if req := h.provider.next(t); !req.HasNack() || codes.Code(req.GetNack().GetErrorDetail().GetCode()) != codes.ResourceExhausted {
		t.Fatalf("provider got %v, want a RESOURCE_EXHAUSTED nack", req)
	}
	n.expectError(t, "a/1", codes.ResourceExhausted)
	// A new version of a key it already has is not a new key.
	h.provider.emitAs(one.GetWatchId(), "n3", "r2", "/one.yaml", oneFile("1b"), nil)
	n.expectUpdates(t, oneFile("1b"), "a/0", "b/0")
	_ = three
}

// A failure is remembered briefly, so a pod that keeps asking does not make
// the server keep asking the provider.
func TestNegativeCache(t *testing.T) {
	now := &clock{now: time.Unix(1_700_000_000, 0)}
	h := startWith(t, Config{Now: now.Now, NegativeTTL: 30 * time.Second}, newFakeProvider())
	n := h.connect(t)

	n.subscribe(t, "a/0", "file:///missing.yaml", "")
	watch := h.provider.expectWatch(t, "file:///missing.yaml")
	h.provider.fail(watch.GetWatchId(), codes.NotFound)
	n.expectError(t, "a/0", codes.NotFound)
	n.unsubscribe(t, "a/0")
	if req := h.provider.next(t); !req.HasUnwatch() {
		t.Fatalf("provider got %v, want the unwatch", req)
	}

	// The pod restarts and asks again: answered from memory.
	n.subscribe(t, "a/0", "file:///missing.yaml", "")
	n.expectError(t, "a/0", codes.NotFound)
	select {
	case req := <-h.provider.requests:
		t.Fatalf("the provider was asked again within the TTL: %v", req)
	case <-time.After(200 * time.Millisecond):
	}
	n.unsubscribe(t, "a/0")

	// After the TTL the provider is asked again.
	now.advance(time.Minute)
	n.subscribe(t, "a/0", "file:///missing.yaml", "")
	again := h.provider.expectWatch(t, "file:///missing.yaml")
	h.provider.emitAs(again.GetWatchId(), "n1", "r1", "/missing.yaml", oneFile("here"), nil)
	n.expectUpdate(t, "a/0", oneFile("here"))
}

// Content may vary only on the attributes the provider declared, and only on
// attributes that cannot identify one pod.
func TestVaryOn(t *testing.T) {
	p := newFakeProvider()
	p.varyOn = []string{"namespace"}
	h := startWith(t, Config{}, p)
	n := h.connect(t)

	n.subscribeAs(t, "a/0", "file:///app.yaml", "shop")
	shop := h.provider.expectWatch(t, "file:///app.yaml")
	if got := shop.GetAttributes(); len(got) != 1 || got["namespace"] != "shop" {
		t.Errorf("watch attributes = %v, want only the namespace", got)
	}
	// Same namespace: same resource, no second watch.
	n.subscribeAs(t, "b/0", "file:///app.yaml", "shop")
	// Another namespace is another resource.
	n.subscribeAs(t, "c/0", "file:///app.yaml", "billing")
	billing := h.provider.expectWatch(t, "file:///app.yaml")
	if billing.GetAttributes()["namespace"] != "billing" {
		t.Errorf("second watch attributes = %v", billing.GetAttributes())
	}

	h.provider.emitAs(shop.GetWatchId(), "n1", "r1", "/app.yaml@shop", oneFile("shop"), nil)
	n.expectUpdates(t, oneFile("shop"), "a/0", "b/0")
	h.provider.emitAs(billing.GetWatchId(), "n2", "r1", "/app.yaml@billing", oneFile("billing"), nil)
	n.expectUpdate(t, "c/0", oneFile("billing"))
}

func TestVaryOnRefusesPodIdentity(t *testing.T) {
	p := newFakeProvider()
	p.varyOn = []string{"pod_name"}
	// The handshake is refused, so the hub never reports the provider
	// connected: run it without waiting.
	h := startUnconnected(t, p)
	n := h.connect(t)
	n.subscribe(t, "a/0", "file:///app.yaml", "")
	n.expectError(t, "a/0", codes.Unavailable)
}

// The variant cap: one URI may not resolve to more keys than the quota.
func TestVariantCap(t *testing.T) {
	p := newFakeProvider()
	p.varyOn = []string{"namespace"}
	h := startWith(t, Config{Quotas: Quotas{
		MaxKeysPerProvider: 100, MaxBytesPerProvider: 1 << 20, MaxVariantsPerURI: 2, MaxURIsPerNamespace: 100,
	}}, p)
	n := h.connect(t)
	for i := range 3 {
		ns := fmt.Sprintf("ns%d", i)
		n.subscribeAs(t, ns+"/0", "file:///app.yaml", ns)
		watch := h.provider.expectWatch(t, "file:///app.yaml")
		h.provider.emitAs(watch.GetWatchId(), fmt.Sprintf("n%d", i), "r1", "/app.yaml@"+ns, oneFile(ns), nil)
		if i < 2 {
			n.expectUpdate(t, ns+"/0", oneFile(ns))
			h.provider.expectAck(t, fmt.Sprintf("n%d", i))
			continue
		}
		if req := h.provider.next(t); !req.HasNack() {
			t.Fatalf("provider got %v, want the third variant refused", req)
		}
		n.expectError(t, ns+"/0", codes.ResourceExhausted)
	}
}

// Entries nothing refers to are dropped after the grace period, with their
// content; entries in use never are.
func TestCollectDropsUnusedEntries(t *testing.T) {
	now := &clock{now: time.Unix(1_700_000_000, 0)}
	dir := t.TempDir()
	h := startWith(t, Config{Now: now.Now, CacheDir: dir, Grace: time.Hour}, newFakeProvider())
	n := h.connect(t)

	for i, name := range []string{"kept", "dropped"} {
		n.subscribe(t, name+"/0", "file:///"+name+".yaml", "")
		watch := h.provider.expectWatch(t, "file:///"+name+".yaml")
		h.provider.emitAs(watch.GetWatchId(), fmt.Sprintf("n%d", i), "r1", "/"+name+".yaml", oneFile(name), nil)
		n.expectUpdate(t, name+"/0", oneFile(name))
		h.provider.expectAck(t, fmt.Sprintf("n%d", i))
	}
	n.unsubscribe(t, "dropped/0")
	if req := h.provider.next(t); !req.HasUnwatch() {
		t.Fatalf("provider got %v, want the unwatch", req)
	}

	entries := func() int {
		h.hub.mu.Lock()
		defer h.hub.mu.Unlock()
		return len(h.hub.entries)
	}
	now.advance(30 * time.Minute)
	h.hub.collect()
	if got := entries(); got != 2 {
		t.Fatalf("%d entries within the grace period, want 2", got)
	}
	now.advance(time.Hour)
	h.hub.collect()
	if got := entries(); got != 1 {
		t.Fatalf("%d entries after the grace period, want only the one in use", got)
	}
	if _, err := h.hub.store.Get(bundle.Version(oneFile("dropped"))); err == nil {
		t.Error("the dropped entry's content is still stored")
	}
	if _, err := h.hub.store.Get(bundle.Version(oneFile("kept"))); err != nil {
		t.Errorf("the content of an entry in use is gone: %v", err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// With verification on, who the pod is comes from its token: a driver cannot
// speak for a pod it holds no token of, nor claim another namespace.
func TestSubscriptionIdentityComesFromTheToken(t *testing.T) {
	p := newFakeProvider()
	p.varyOn = []string{"namespace"}
	h := startWith(t, Config{Verify: func(token string) (Identity, error) {
		if token != "good" {
			return Identity{}, fmt.Errorf("invalid service account token: %q", token)
		}
		return Identity{Namespace: "shop", ServiceAccount: "web", PodName: "web-0", PodUID: "uid-1"}, nil
	}}, p)
	n := h.connect(t)
	subscribe := func(id, token, claimedNamespace string) {
		t.Helper()
		n.send(t, chandlerv1.ConfigRequest_builder{
			Subscribe: chandlerv1.Subscribe_builder{
				SubscriptionId: proto.String(id),
				Uri:            proto.String("file:///app.yaml"),
				Workload: chandlerv1.Workload_builder{
					Namespace:           proto.String(claimedNamespace),
					ServiceAccountToken: proto.String(token),
				}.Build(),
			}.Build(),
		}.Build())
	}

	subscribe("none/0", "", "shop")
	n.expectError(t, "none/0", codes.Unauthenticated)
	subscribe("bad/0", "forged", "shop")
	n.expectError(t, "bad/0", codes.Unauthenticated)

	// The driver claims another namespace; the token's is what counts.
	subscribe("ok/0", "good", "kube-system")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	if got := watch.GetAttributes()["namespace"]; got != "shop" {
		t.Errorf("the provider was asked on behalf of namespace %q, want the token's", got)
	}
}
