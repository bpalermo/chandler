package provider

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/rollout"
	"github.com/bpalermo/chandler/common/uri"
)

const wait = 10 * time.Second

// memory is an Origin held in a map: every path is a one-file resource whose
// revision is a counter, so a test can tell a refetch from a change.
type memory struct {
	mu      sync.Mutex
	content map[string]string
	serial  map[string]int
	// since is when each path's current content appeared; unset paths have
	// no time, and so no rollout.
	since map[string]time.Time
	// hold caps the share of each path, in basis points.
	hold map[string]uint32
}

func newMemory() *memory {
	return &memory{
		content: map[string]string{}, serial: map[string]int{},
		since: map[string]time.Time{}, hold: map[string]uint32{},
	}
}

func (m *memory) put(path, content string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.content[path] = content
	m.serial[path]++
}

func (m *memory) remove(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.content, path)
}

func (m *memory) Resolve(u uri.URI) (Resource, error) {
	if u.Authority() != "" || len(u.Query()) != 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalid, u)
	}
	return memoryResource{origin: m, path: u.Path()}, nil
}

type memoryResource struct {
	origin *memory
	path   string
}

func (r memoryResource) CacheKey() string { return r.path }

func (r memoryResource) Fetch(_ context.Context, known string, limits bundle.Limits) (State, error) {
	r.origin.mu.Lock()
	defer r.origin.mu.Unlock()
	content, ok := r.origin.content[r.path]
	if !ok {
		return State{}, fmt.Errorf("%w: %s", ErrNotFound, r.path)
	}
	if uint64(len(content)) > limits.MaxBytes {
		return State{}, fmt.Errorf("%w: %s", bundle.ErrTooLarge, r.path)
	}
	revision := fmt.Sprintf("r%d", r.origin.serial[r.path])
	state := State{Revision: revision, Since: r.origin.since[r.path]}
	if hold, ok := r.origin.hold[r.path]; ok {
		state.Hold = &hold
	}
	if revision == known {
		return state, nil
	}
	b := chandlerv1.Bundle_builder{Files: []*chandlerv1.File{
		chandlerv1.File_builder{Path: proto.String("f"), Content: []byte(content)}.Build(),
	}}.Build()
	state.Bundle = b
	return state, nil
}

type watcher struct {
	t      *testing.T
	stream chandlerv1.ConfigProvider_StreamConfigsClient
	in     chan *chandlerv1.ProviderResponse
}

// start serves origin as provider "file" and opens a stream to it with the
// given size limit.
func start(t *testing.T, origin Origin, maxBytes uint64, configure ...func(*Server)) *watcher {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	provider := &Server{
		Name:         "file",
		Origin:       origin,
		PollInterval: 10 * time.Millisecond,
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, c := range configure {
		c(provider)
	}
	chandlerv1.RegisterConfigProviderServer(server, provider)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := chandlerv1.NewConfigProviderClient(conn).StreamConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := &watcher{t: t, stream: stream, in: make(chan *chandlerv1.ProviderResponse, 64)}
	w.send(chandlerv1.ProviderRequest_builder{
		Hello: chandlerv1.WatcherHello_builder{
			ServerId:       proto.String("server-1"),
			MaxBundleBytes: proto.Uint64(maxBytes),
		}.Build(),
	}.Build())
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			w.in <- msg
		}
	}()
	if hello := w.next(); !hello.HasHello() || hello.GetHello().GetName() != "file" {
		t.Fatalf("first message = %v, want the ProviderHello", hello)
	}
	return w
}

func (w *watcher) send(req *chandlerv1.ProviderRequest) {
	w.t.Helper()
	if err := w.stream.Send(req); err != nil {
		w.t.Fatal(err)
	}
}

func (w *watcher) watch(id, uri, known string) {
	w.t.Helper()
	watch := chandlerv1.Watch_builder{WatchId: proto.String(id), Uri: proto.String(uri)}
	if known != "" {
		watch.KnownRevision = proto.String(known)
	}
	w.send(chandlerv1.ProviderRequest_builder{Watch: watch.Build()}.Build())
}

func (w *watcher) next() *chandlerv1.ProviderResponse {
	w.t.Helper()
	select {
	case msg := <-w.in:
		return msg
	case <-time.After(wait):
		w.t.Fatal("the provider sent nothing")
		return nil
	}
}

func (w *watcher) update(id string) *chandlerv1.ResourceUpdate {
	w.t.Helper()
	msg := w.next()
	if !msg.HasUpdate() || msg.GetUpdate().GetWatchId() != id {
		w.t.Fatalf("got %v, want an update of watch %s", msg, id)
	}
	return msg.GetUpdate()
}

func (w *watcher) failure(id string, code codes.Code) {
	w.t.Helper()
	msg := w.next()
	if !msg.HasError() || msg.GetError().GetWatchId() != id || codes.Code(msg.GetError().GetStatus().GetCode()) != code {
		w.t.Fatalf("got %v, want a %s error for watch %s", msg, code, id)
	}
}

func (w *watcher) quiet() {
	w.t.Helper()
	select {
	case msg := <-w.in:
		w.t.Fatalf("unexpected %v", msg)
	case <-time.After(100 * time.Millisecond): // ten poll intervals
	}
}

func TestWatchFollowsAFile(t *testing.T) {
	origin := newMemory()
	origin.put("/app.yaml", "v1")
	w := start(t, origin, 0)

	w.watch("1", "file:///app.yaml", "")
	first := w.update("1")
	if !first.HasBundle() || string(first.GetBundle().GetFiles()[0].GetContent()) != "v1" {
		t.Fatalf("first update = %v", first)
	}
	if first.GetCacheKey() != "/app.yaml" || first.GetRevision() != "r1" || first.GetNonce() == "" {
		t.Errorf("key %q revision %q nonce %q", first.GetCacheKey(), first.GetRevision(), first.GetNonce())
	}
	w.quiet() // unchanged: nothing more

	origin.put("/app.yaml", "v2")
	second := w.update("1")
	if string(second.GetBundle().GetFiles()[0].GetContent()) != "v2" || second.GetNonce() == first.GetNonce() {
		t.Errorf("second update = %v", second)
	}

	// Gone: reported once, and the content returns when the file does.
	origin.remove("/app.yaml")
	if deleted := w.update("1"); !deleted.HasDeleted() {
		t.Errorf("after removal got %v, want Deleted", deleted)
	}
	w.quiet()
	origin.put("/app.yaml", "v3")
	if back := w.update("1"); string(back.GetBundle().GetFiles()[0].GetContent()) != "v3" {
		t.Errorf("after the file returned got %v", back)
	}

	// After an unwatch the provider says nothing more about it.
	w.send(chandlerv1.ProviderRequest_builder{
		Unwatch: chandlerv1.Unwatch_builder{WatchId: proto.String("1")}.Build(),
	}.Build())
	time.Sleep(50 * time.Millisecond)
	origin.put("/app.yaml", "v4")
	w.quiet()
}

func TestWatchWithKnownRevision(t *testing.T) {
	origin := newMemory()
	origin.put("/flags/", "a")
	w := start(t, origin, 0)

	w.watch("1", "file:///flags/", "")
	revision := w.update("1").GetRevision()

	// The server already holds this revision: no bytes are sent again.
	w.watch("2", "file:///flags/", revision)
	if again := w.update("2"); !again.HasNotModified() || again.GetRevision() != revision {
		t.Errorf("got %v, want NotModified at %s", again, revision)
	}
	// It holds an older one: the bundle follows.
	w.watch("3", "file:///flags/", "r0")
	if fresh := w.update("3"); !fresh.HasBundle() {
		t.Errorf("got %v, want the bundle", fresh)
	}
}

func TestWatchErrors(t *testing.T) {
	origin := newMemory()
	origin.put("/big.bin", "0123456789")
	w := start(t, origin, 4)

	// Missing: reported once, and delivered when it appears.
	w.watch("1", "file:///later.yaml", "")
	w.failure("1", codes.NotFound)
	w.quiet()
	origin.put("/later.yaml", "ok")
	if u := w.update("1"); !u.HasBundle() {
		t.Errorf("got %v, want the bundle once the file exists", u)
	}

	// Over the server's limit: never read into a bundle.
	w.watch("2", "file:///big.bin", "")
	w.failure("2", codes.ResourceExhausted)

	// URIs the provider cannot interpret end the watch at once. A query
	// parameter is refused, not ignored: it would name the same file twice.
	for id, uri := range map[string]string{
		"3": "file:///later.yaml?version=2",
		"4": "file://host/later.yaml",
		"5": "s3:///later.yaml",
		"6": "not a uri",
	} {
		w.watch(id, uri, "")
		w.failure(id, codes.InvalidArgument)
	}
}

// fakeClock is a settable time source.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func share(u *chandlerv1.ResourceUpdate) uint32 {
	if !u.HasRollout() {
		return rollout.Everyone
	}
	return u.GetRollout().GetBasisPoints()
}

// The provider owns the rollout: it releases a revision to a share of the
// pods, raises the share as time passes without sending the bytes again, and
// then releases it to everyone.
func TestRampOverTime(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	ramp, err := rollout.ParseRamp("10%:1m,50%:1m")
	if err != nil {
		t.Fatal(err)
	}
	origin := newMemory()
	origin.put("/app.yaml", "v1")
	origin.since["/app.yaml"] = clock.Now()
	w := start(t, origin, 0, func(s *Server) { s.Ramp, s.Now = ramp, clock.Now })

	w.watch("1", "file:///app.yaml", "")
	first := w.update("1")
	if !first.HasBundle() || share(first) != 1000 {
		t.Fatalf("first update = %v, want the bundle at 10%%", first)
	}
	w.quiet() // same share: nothing to say

	clock.advance(time.Minute)
	second := w.update("1")
	if !second.HasNotModified() || second.GetRevision() != first.GetRevision() || share(second) != 5000 {
		t.Fatalf("after the first hold got %v, want the same revision at 50%% and no bytes", second)
	}

	clock.advance(time.Minute)
	if released := w.update("1"); !released.HasNotModified() || released.HasRollout() {
		t.Fatalf("after the last hold got %v, want the revision released to everyone", released)
	}
	w.quiet()

	// A server that reconnects mid-rollout is told where it stands.
	origin.mu.Lock()
	origin.content["/app.yaml"] = "v2"
	origin.serial["/app.yaml"]++
	origin.since["/app.yaml"] = clock.Now()
	origin.mu.Unlock()
	candidate := w.update("1")
	w.watch("2", "file:///app.yaml", candidate.GetRevision())
	if again := w.update("2"); !again.HasNotModified() || share(again) != 1000 {
		t.Errorf("a watch that knows the revision got %v, want NotModified at 10%%", again)
	}
}

// An origin can hold a rollout below where time would have taken it.
func TestRampHeldByTheOrigin(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	ramp, _ := rollout.ParseRamp("10%:1m,50%:1m")
	origin := newMemory()
	origin.put("/app.yaml", "v1")
	origin.since["/app.yaml"] = clock.Now()
	origin.hold["/app.yaml"] = 1000
	w := start(t, origin, 0, func(s *Server) { s.Ramp, s.Now = ramp, clock.Now })

	w.watch("1", "file:///app.yaml", "")
	if u := w.update("1"); share(u) != 1000 {
		t.Fatalf("first update at %d, want 10%%", share(u))
	}
	clock.advance(10 * time.Minute)
	w.quiet() // held at 10% however long it has been

	origin.mu.Lock()
	delete(origin.hold, "/app.yaml")
	origin.mu.Unlock()
	if u := w.update("1"); u.HasRollout() {
		t.Errorf("once released got %v, want the revision for everyone", u)
	}
}

// When pods refuse a revision, its rollout stops where it is: the pods that
// have it keep it and no more get it, until the refusals are gone.
func TestRampHeldWhenPodsRefuse(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	ramp, _ := rollout.ParseRamp("10%:1m,50%:1m")
	origin := newMemory()
	origin.put("/app.yaml", "v1")
	origin.since["/app.yaml"] = clock.Now()
	w := start(t, origin, 0, func(s *Server) {
		s.Ramp, s.Now, s.MaxOverdue, s.HoldGrace = ramp, clock.Now, 1, time.Minute
	})

	w.watch("1", "file:///app.yaml", "")
	first := w.update("1")
	overdue := uint32(0)
	report := func(refused, rejected uint32) {
		w.send(chandlerv1.ProviderRequest_builder{
			Feedback: chandlerv1.Feedback_builder{
				CacheKey: proto.String("/app.yaml"),
				Revision: proto.String(first.GetRevision()),
				Assigned: proto.Uint32(5),
				Refused:  proto.Uint32(refused),
				Rejected: proto.Uint32(rejected),
				Overdue:  proto.Uint32(overdue),
			}.Build(),
		}.Build())
		time.Sleep(50 * time.Millisecond) // let the provider read it
	}

	report(1, 0)
	clock.advance(time.Minute)
	w.quiet() // would be 50% by now; held at 10%

	// An application rejecting the content holds it just the same.
	report(0, 1)
	clock.advance(time.Minute)
	w.quiet()

	// Applications that do not report on it at all hold it too, past the
	// number the provider tolerates (one, here).
	overdue = 2
	report(0, 0)
	clock.advance(time.Minute)
	w.quiet()
	overdue = 1

	// The reports clear, but a clear report may only be the gap while a
	// driver reconnects: the hold stays for the grace period...
	report(0, 0)
	clock.advance(30 * time.Second)
	w.quiet()
	// ...and a refusal seen again within it starts the period over.
	report(1, 0)
	report(0, 0)
	clock.advance(45 * time.Second)
	w.quiet()

	clock.advance(30 * time.Second)
	if u := w.update("1"); u.HasRollout() {
		t.Errorf("with the refusals gone for the grace period got %v, want the rollout to go on to everyone", u)
	}
}

// A revision with no time at the origin is not rolled out.
func TestNoRampWithoutATime(t *testing.T) {
	ramp, _ := rollout.ParseRamp("10%:1h")
	origin := newMemory()
	origin.put("/app.yaml", "v1")
	w := start(t, origin, 0, func(s *Server) { s.Ramp = ramp })
	w.watch("1", "file:///app.yaml", "")
	if u := w.update("1"); u.HasRollout() {
		t.Errorf("got %v, want the revision for everyone", u)
	}
}
