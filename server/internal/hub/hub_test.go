package hub

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

const wait = 10 * time.Second

// fakeProvider is a ConfigProvider driven by the test: it exposes what the
// server sends and sends what the test tells it to.
type fakeProvider struct {
	chandlerv1.UnimplementedConfigProviderServer
	name string
	// requests carries everything the server sends after its hello.
	requests chan *chandlerv1.ProviderRequest
	// responses is sent on the current stream.
	responses chan *chandlerv1.ProviderResponse
	// hangup ends the current stream.
	hangup chan struct{}
}

func (p *fakeProvider) StreamConfigs(stream chandlerv1.ConfigProvider_StreamConfigsServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	err := stream.Send(chandlerv1.ProviderResponse_builder{
		Hello: chandlerv1.ProviderHello_builder{Name: proto.String(p.name)}.Build(),
	}.Build())
	if err != nil {
		return err
	}
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				return
			}
			p.requests <- req
		}
	}()
	for {
		select {
		case <-p.hangup:
			return io.ErrUnexpectedEOF
		case <-stream.Context().Done():
			return nil
		case msg := <-p.responses:
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (p *fakeProvider) next(t *testing.T) *chandlerv1.ProviderRequest {
	t.Helper()
	select {
	case req := <-p.requests:
		return req
	case <-time.After(wait):
		t.Fatal("the server sent nothing to the provider")
		return nil
	}
}

func (p *fakeProvider) expectWatch(t *testing.T, uri string) *chandlerv1.Watch {
	t.Helper()
	req := p.next(t)
	if !req.HasWatch() || req.GetWatch().GetUri() != uri {
		t.Fatalf("provider got %v, want a Watch of %s", req, uri)
	}
	return req.GetWatch()
}

func oneFile(content string) *chandlerv1.Bundle {
	return chandlerv1.Bundle_builder{Files: []*chandlerv1.File{
		chandlerv1.File_builder{Path: proto.String("app.yaml"), Content: []byte(content)}.Build(),
	}}.Build()
}

func (p *fakeProvider) emit(watchID, nonce, revision string, b *chandlerv1.Bundle) {
	p.responses <- chandlerv1.ProviderResponse_builder{
		Update: chandlerv1.ResourceUpdate_builder{
			WatchId:  proto.String(watchID),
			Nonce:    proto.String(nonce),
			CacheKey: proto.String("/app.yaml"),
			Revision: proto.String(revision),
			Bundle:   b,
		}.Build(),
	}.Build()
}

// node is a driver's stream to the server.
type node struct {
	stream chandlerv1.ConfigDiscoveryService_StreamConfigsClient
	in     chan *chandlerv1.ConfigResponse
}

func (n *node) send(t *testing.T, req *chandlerv1.ConfigRequest) {
	t.Helper()
	if err := n.stream.Send(req); err != nil {
		t.Fatal(err)
	}
}

func (n *node) subscribe(t *testing.T, id, uri, applied string) {
	t.Helper()
	sub := chandlerv1.Subscribe_builder{SubscriptionId: proto.String(id), Uri: proto.String(uri)}
	if applied != "" {
		sub.AppliedVersion = proto.String(applied)
	}
	n.send(t, chandlerv1.ConfigRequest_builder{Subscribe: sub.Build()}.Build())
}

func (n *node) next(t *testing.T) *chandlerv1.ConfigResponse {
	t.Helper()
	select {
	case msg, ok := <-n.in:
		if !ok {
			t.Fatal("the server closed the stream")
		}
		return msg
	case <-time.After(wait):
		t.Fatal("the server sent nothing to the driver")
		return nil
	}
}

func (n *node) expectUpdate(t *testing.T, id string, b *chandlerv1.Bundle) *chandlerv1.ConfigUpdate {
	t.Helper()
	msg := n.next(t)
	u := msg.GetUpdate()
	if !msg.HasUpdate() || u.GetSubscriptionId() != id || u.GetVersion() != bundle.Version(b) {
		t.Fatalf("driver got %v, want an update of %s to %s", msg, id, bundle.Version(b))
	}
	return u
}

func (n *node) expectError(t *testing.T, id string, code codes.Code) *status.Status {
	t.Helper()
	msg := n.next(t)
	e := msg.GetError()
	if !msg.HasError() || e.GetSubscriptionId() != id || codes.Code(e.GetStatus().GetCode()) != code {
		t.Fatalf("driver got %v, want a %s error for %s", msg, code, id)
	}
	return e.GetStatus()
}

func (n *node) expectNothing(t *testing.T) {
	t.Helper()
	select {
	case msg := <-n.in:
		t.Fatalf("driver got an unexpected %v", msg)
	case <-time.After(200 * time.Millisecond):
	}
}

type harness struct {
	provider *fakeProvider
	target   string
}

func serve(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// start runs a hub with one fake provider registered as "file".
func start(t *testing.T, limits bundle.Limits) harness {
	t.Helper()
	p := &fakeProvider{
		name:      "file",
		requests:  make(chan *chandlerv1.ProviderRequest, 64),
		responses: make(chan *chandlerv1.ProviderResponse),
		hangup:    make(chan struct{}),
	}
	providerTarget := serve(t, func(s *grpc.Server) { chandlerv1.RegisterConfigProviderServer(s, p) })

	h := New(Config{ServerID: "server-1", Limits: limits, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := h.AddProvider("file", providerTarget); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.Run(ctx)
	target := serve(t, func(s *grpc.Server) { chandlerv1.RegisterConfigDiscoveryServiceServer(s, h) })

	// Wait for the provider stream, so tests start from a connected server.
	deadline := time.Now().Add(wait)
	for {
		h.mu.Lock()
		connected := h.upstreams["file"].connected()
		h.mu.Unlock()
		if connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the server never connected to the provider")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return harness{provider: p, target: target}
}

func (h harness) connect(t *testing.T) *node {
	t.Helper()
	conn, err := grpc.NewClient(h.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := chandlerv1.NewConfigDiscoveryServiceClient(conn).StreamConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := &node{stream: stream, in: make(chan *chandlerv1.ConfigResponse, 64)}
	n.send(t, chandlerv1.ConfigRequest_builder{
		Hello: chandlerv1.NodeHello_builder{NodeId: proto.String("node-1")}.Build(),
	}.Build())
	go func() {
		defer close(n.in)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			n.in <- msg
		}
	}()
	if hello := n.next(t); !hello.HasHello() || hello.GetHello().GetServerId() != "server-1" {
		t.Fatalf("first message = %v, want the ServerHello", hello)
	}
	return n
}

func ack(u *chandlerv1.ConfigUpdate) *chandlerv1.ConfigRequest {
	return chandlerv1.ConfigRequest_builder{
		Ack: chandlerv1.Ack_builder{
			SubscriptionId: proto.String(u.GetSubscriptionId()),
			Nonce:          proto.String(u.GetNonce()),
			Version:        proto.String(u.GetVersion()),
		}.Build(),
	}.Build()
}

func nack(u *chandlerv1.ConfigUpdate, code codes.Code) *chandlerv1.ConfigRequest {
	return chandlerv1.ConfigRequest_builder{
		Nack: chandlerv1.Nack_builder{
			SubscriptionId: proto.String(u.GetSubscriptionId()),
			Nonce:          proto.String(u.GetNonce()),
			Version:        proto.String(u.GetVersion()),
			ErrorDetail:    &status.Status{Code: int32(code), Message: "test"},
		}.Build(),
	}.Build()
}

func TestSubscriptionsShareOneWatchAndFollowRevisions(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	a, b := h.connect(t), h.connect(t)

	a.subscribe(t, "vol-a/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	if watch.GetKnownRevision() != "" {
		t.Errorf("first watch carries known_revision %q", watch.GetKnownRevision())
	}
	a.expectNothing(t) // the answer waits for the provider

	v1 := oneFile("v1")
	h.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	update := a.expectUpdate(t, "vol-a/0", v1)
	if !update.HasBundle() || string(update.GetBundle().GetFiles()[0].GetContent()) != "v1" {
		t.Errorf("update carries %v, want the bundle", update)
	}
	if req := h.provider.next(t); !req.HasAck() || req.GetAck().GetNonce() != "n1" || req.GetAck().GetRevision() != "r1" {
		t.Errorf("provider got %v, want the ack of n1", req)
	}
	a.send(t, ack(update))

	// Another spelling of the same URI is the same resource: answered from
	// what the server holds, with no second watch.
	b.subscribe(t, "vol-b/0", "FILE:///x/../app.yaml", "")
	b.expectUpdate(t, "vol-b/0", v1)

	v2 := oneFile("v2")
	h.provider.emit(watch.GetWatchId(), "n2", "r2", v2)
	a.expectUpdate(t, "vol-a/0", v2)
	b.expectUpdate(t, "vol-b/0", v2)
	if req := h.provider.next(t); !req.HasAck() || req.GetAck().GetNonce() != "n2" {
		t.Errorf("provider got %v, want the ack of n2 and no second watch", req)
	}

	// The watch ends with the last subscription.
	a.send(t, chandlerv1.ConfigRequest_builder{
		Unsubscribe: chandlerv1.Unsubscribe_builder{SubscriptionId: proto.String("vol-a/0")}.Build(),
	}.Build())
	b.stream.CloseSend()
	if req := h.provider.next(t); !req.HasUnwatch() || req.GetUnwatch().GetWatchId() != watch.GetWatchId() {
		t.Errorf("provider got %v, want the unwatch", req)
	}
}

func TestProviderFailuresAnswerThePendingSubscription(t *testing.T) {
	h := start(t, bundle.Limits{MaxBytes: 8, MaxFiles: 4})
	n := h.connect(t)

	n.subscribe(t, "vol/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")

	// The provider has nothing: the driver is told, and stays subscribed.
	h.provider.responses <- chandlerv1.ProviderResponse_builder{
		Error: chandlerv1.WatchError_builder{
			WatchId: proto.String(watch.GetWatchId()),
			Status:  &status.Status{Code: int32(codes.NotFound), Message: "no such file"},
		}.Build(),
	}.Build()
	if st := n.expectError(t, "vol/0", codes.NotFound); st.GetMessage() != "no such file" {
		t.Errorf("error message = %q", st.GetMessage())
	}

	// A bundle over the limits is refused to the provider, and reaches no pod.
	h.provider.emit(watch.GetWatchId(), "n1", "r1", oneFile("far too large"))
	req := h.provider.next(t)
	if !req.HasNack() || codes.Code(req.GetNack().GetErrorDetail().GetCode()) != codes.ResourceExhausted {
		t.Fatalf("provider got %v, want a RESOURCE_EXHAUSTED nack", req)
	}
	n.expectNothing(t)

	// A deliverable revision follows the error.
	small := oneFile("ok")
	h.provider.emit(watch.GetWatchId(), "n2", "r2", small)
	n.expectUpdate(t, "vol/0", small)
}

func TestSubscribeErrors(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	n := h.connect(t)

	n.subscribe(t, "vol/0", "s3://bucket/key", "")
	if st := n.expectError(t, "vol/0", codes.NotFound); !strings.Contains(st.GetMessage(), "s3://") {
		t.Errorf("message %q does not name the scheme", st.GetMessage())
	}
	n.subscribe(t, "vol/1", "not a uri", "")
	n.expectError(t, "vol/1", codes.InvalidArgument)
}

func TestUseCachedAndResendOnDataLoss(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	first := h.connect(t)
	v1 := oneFile("v1")

	first.subscribe(t, "vol-a/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	first.expectUpdate(t, "vol-a/0", v1)

	// A driver that already has the version is told to keep it: no bytes.
	second := h.connect(t)
	second.subscribe(t, "vol-b/0", "file:///app.yaml", bundle.Version(v1))
	update := second.expectUpdate(t, "vol-b/0", v1)
	if !update.HasUseCached() {
		t.Fatalf("update carries %v, want UseCached", update)
	}

	// If it turns out not to have it, the bundle itself follows.
	second.send(t, nack(update, codes.DataLoss))
	resent := second.expectUpdate(t, "vol-b/0", v1)
	if !resent.HasBundle() || resent.GetNonce() == update.GetNonce() {
		t.Errorf("after DATA_LOSS got %v, want the bundle under a new nonce", resent)
	}

	// A bundle the volume can never take is not sent again.
	second.send(t, nack(resent, codes.FailedPrecondition))
	second.expectNothing(t)
}

func TestRetryAfterNodeSideFailure(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	n := h.connect(t)
	v1 := oneFile("v1")

	n.subscribe(t, "vol/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	update := n.expectUpdate(t, "vol/0", v1)

	n.send(t, nack(update, codes.Internal))
	retried := n.expectUpdate(t, "vol/0", v1) // after the first backoff step
	if !retried.HasBundle() || retried.GetNonce() == update.GetNonce() {
		t.Errorf("retry = %v, want the bundle under a new nonce", retried)
	}
	n.send(t, ack(retried))
	n.expectNothing(t)
}

// A provider outage takes nothing away: what the server holds keeps being
// served, and the watch resumes from the revision it has.
func TestProviderOutage(t *testing.T) {
	h := start(t, bundle.DefaultLimits())
	n := h.connect(t)
	v1 := oneFile("v1")

	n.subscribe(t, "vol/0", "file:///app.yaml", "")
	watch := h.provider.expectWatch(t, "file:///app.yaml")
	h.provider.emit(watch.GetWatchId(), "n1", "r1", v1)
	n.expectUpdate(t, "vol/0", v1)
	h.provider.next(t) // the ack

	h.provider.hangup <- struct{}{}
	// Wait until the server has noticed.
	late := h.connect(t)
	deadline := time.Now().Add(wait)
	for {
		late.subscribe(t, "probe/0", "file:///other.yaml", "")
		msg := late.next(t)
		if msg.HasError() && codes.Code(msg.GetError().GetStatus().GetCode()) == codes.Unavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a new resource was never reported unavailable during the outage")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The held version still answers a new subscription.
	late.subscribe(t, "vol-late/0", "file:///app.yaml", "")
	late.expectUpdate(t, "vol-late/0", v1)

	// On reconnect every resource is watched again, with what is held.
	seen := map[string]string{}
	for len(seen) < 2 {
		req := h.provider.next(t)
		if req.HasWatch() {
			seen[req.GetWatch().GetUri()] = req.GetWatch().GetKnownRevision()
		}
	}
	if seen["file:///app.yaml"] != "r1" {
		t.Errorf("re-watched app.yaml with known_revision %q, want r1", seen["file:///app.yaml"])
	}
	if seen["file:///other.yaml"] != "" {
		t.Errorf("re-watched other.yaml with known_revision %q, want none", seen["file:///other.yaml"])
	}
}
