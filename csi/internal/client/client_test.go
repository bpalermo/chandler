package client

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
)

const wait = 10 * time.Second

// fakeServer is a ConfigDiscoveryService driven by the test.
type fakeServer struct {
	chandlerv1.UnimplementedConfigDiscoveryServiceServer
	requests  chan *chandlerv1.ConfigRequest
	responses chan *chandlerv1.ConfigResponse
	hangup    chan struct{}
}

func (f *fakeServer) StreamConfigs(stream chandlerv1.ConfigDiscoveryService_StreamConfigsServer) error {
	hello, err := stream.Recv()
	if err != nil {
		return err
	}
	f.requests <- hello
	err = stream.Send(chandlerv1.ConfigResponse_builder{
		Hello: chandlerv1.ServerHello_builder{ServerId: proto.String("server-1")}.Build(),
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
			f.requests <- req
		}
	}()
	for {
		select {
		case <-f.hangup:
			return io.ErrUnexpectedEOF
		case <-stream.Context().Done():
			return nil
		case msg := <-f.responses:
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (f *fakeServer) next(t *testing.T) *chandlerv1.ConfigRequest {
	t.Helper()
	select {
	case req := <-f.requests:
		return req
	case <-time.After(wait):
		t.Fatal("the driver sent nothing")
		return nil
	}
}

func (f *fakeServer) update(id, nonce, version string) {
	f.responses <- chandlerv1.ConfigResponse_builder{
		Update: chandlerv1.ConfigUpdate_builder{
			SubscriptionId: proto.String(id),
			Nonce:          proto.String(nonce),
			Version:        proto.String(version),
			Bundle:         &chandlerv1.Bundle{},
		}.Build(),
	}.Build()
}

// handler records what it is given and refuses the versions it is told to.
type handler struct {
	mu      sync.Mutex
	applied string
	refuse  map[string]error
	failed  chan *status.Status
}

func (h *handler) Apply(u *chandlerv1.ConfigUpdate) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.refuse[u.GetVersion()]; err != nil {
		return err
	}
	h.applied = u.GetVersion()
	return nil
}

func (h *handler) Fail(st *status.Status) { h.failed <- st }

func (h *handler) Cached() []string   { return []string{"v0", h.Applied()} }
func (h *handler) Loaded() string     { return "" }
func (h *handler) Token() string      { return "pod-token" }
func (h *handler) Rejected() []string { return nil }
func (h *handler) Overdue() string    { return "" }

func (h *handler) Applied() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.applied
}

func start(t *testing.T) (*fakeServer, *Client) {
	t.Helper()
	f := &fakeServer{
		requests:  make(chan *chandlerv1.ConfigRequest, 64),
		responses: make(chan *chandlerv1.ConfigResponse),
		hangup:    make(chan struct{}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	chandlerv1.RegisterConfigDiscoveryServiceServer(server, f)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	c := New(Config{
		Target:  listener.Addr().String(),
		NodeID:  "node-1",
		Version: "test",
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if c.Err() == nil {
		t.Error("a client that never connected reports no error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c.Run(ctx)

	hello := f.next(t)
	if !hello.HasHello() || hello.GetHello().GetNodeId() != "node-1" || hello.GetHello().GetDriverVersion() != "test" {
		t.Fatalf("first message = %v, want the NodeHello", hello)
	}
	return f, c
}

func TestSubscribeAckNackAndResync(t *testing.T) {
	f, c := start(t)
	h := &handler{
		refuse: map[string]error{"v2": grpcstatus.Error(codes.ResourceExhausted, "disk full")},
		failed: make(chan *status.Status, 1),
	}
	workload := chandlerv1.Workload_builder{PodName: proto.String("web-0")}.Build()
	c.Subscribe(Subscription{ID: "vol/0", URI: "file:///app.yaml", Workload: workload, Handler: h})

	sub := f.next(t).GetSubscribe()
	if sub.GetSubscriptionId() != "vol/0" || sub.GetUri() != "file:///app.yaml" ||
		sub.GetWorkload().GetPodName() != "web-0" || sub.GetWorkload().GetServiceAccountToken() != "pod-token" ||
		sub.HasAppliedVersion() ||
		len(sub.GetCachedVersions()) != 1 || sub.GetCachedVersions()[0] != "v0" {
		t.Fatalf("subscribe = %v", sub)
	}

	// Written: ACK.
	f.update("vol/0", "n1", "v1")
	if ack := f.next(t).GetAck(); ack.GetNonce() != "n1" || ack.GetVersion() != "v1" || ack.GetSubscriptionId() != "vol/0" {
		t.Errorf("after a written update got %v, want the ack of n1", ack)
	}

	// Not written: NACK with the handler's code and what is still applied.
	f.update("vol/0", "n2", "v2")
	nack := f.next(t).GetNack()
	if nack.GetNonce() != "n2" || nack.GetVersion() != "v2" || nack.GetAppliedVersion() != "v1" ||
		codes.Code(nack.GetErrorDetail().GetCode()) != codes.ResourceExhausted || nack.GetErrorDetail().GetMessage() != "disk full" {
		t.Errorf("after a refused update got %v", nack)
	}

	// An error from the server reaches the handler.
	f.responses <- chandlerv1.ConfigResponse_builder{
		Error: chandlerv1.SubscriptionError_builder{
			SubscriptionId: proto.String("vol/0"),
			Status:         &status.Status{Code: int32(codes.NotFound), Message: "gone"},
		}.Build(),
	}.Build()
	select {
	case st := <-h.failed:
		if codes.Code(st.GetCode()) != codes.NotFound {
			t.Errorf("handler got %v", st)
		}
	case <-time.After(wait):
		t.Fatal("the handler was not told about the error")
	}

	// An update for a subscription that does not exist is dropped silently.
	f.update("other/0", "n3", "v9")

	// A broken stream is a full resync: hello, then every subscription with
	// the version it has.
	f.hangup <- struct{}{}
	if hello := f.next(t); !hello.HasHello() {
		t.Fatalf("after reconnect got %v, want a NodeHello", hello)
	}
	if c.Err() != nil {
		// The handshake finished before the hello reached the test, or is
		// about to; either way the resubscribe below proves the connection.
		t.Log("still reconnecting:", c.Err())
	}
	// The applied version is not repeated among the cached ones.
	if sub := f.next(t).GetSubscribe(); sub.GetSubscriptionId() != "vol/0" || sub.GetAppliedVersion() != "v1" ||
		len(sub.GetCachedVersions()) != 1 || sub.GetCachedVersions()[0] != "v0" {
		t.Errorf("resubscribe = %v, want vol/0 with applied version v1 and v0 cached", sub)
	}

	c.Unsubscribe("vol/0")
	if un := f.next(t); !un.HasUnsubscribe() || un.GetUnsubscribe().GetSubscriptionId() != "vol/0" {
		t.Errorf("got %v, want the unsubscribe", un)
	}
	c.Unsubscribe("vol/0") // unknown now: nothing is sent
	select {
	case req := <-f.requests:
		t.Errorf("unexpected %v", req)
	case <-time.After(100 * time.Millisecond):
	}
}
