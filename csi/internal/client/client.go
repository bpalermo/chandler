// Package client holds the driver's stream to the config server. One stream
// carries every subscription on the node; when it breaks, the client
// reconnects and subscribes everything again, telling the server which version
// each subscription already has.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/queue"
)

const (
	reconnectBase = 500 * time.Millisecond
	reconnectMax  = 30 * time.Second
)

// Handler receives what the server sends for one subscription. Its methods
// are called from the stream's receive loop, one at a time per client.
type Handler interface {
	// Apply writes an update. A nil error is ACKed. Any other error is
	// NACKed with its gRPC status code, and must have left the previously
	// written version untouched.
	Apply(update *chandlerv1.ConfigUpdate) error
	// Fail reports that the server cannot produce a version for now.
	Fail(st *status.Status)
	// Applied returns the version currently written, "" for none.
	Applied() string
	// Cached returns the versions the node cache holds, newest first. The
	// server may name one of them instead of sending its bytes.
	Cached() []string
	// Loaded returns the last version the application reported loaded, ""
	// for none or when the volume has no application acknowledgement.
	Loaded() string
}

// Subscription is one source of one pod volume.
type Subscription struct {
	// ID is unique on the node: "<csi-volume-id>/<source-index>".
	ID       string
	URI      string
	Workload *chandlerv1.Workload
	// AppAck is set when the application reports on the versions written.
	AppAck  bool
	Handler Handler
}

// Config configures a Client.
type Config struct {
	// Target is the gRPC target of the config server.
	Target string
	// NodeID is the Kubernetes node name.
	NodeID string
	// Version is the driver's version, reported to the server.
	Version string
	Log     *slog.Logger
}

// Client is the driver's connection to the config server.
type Client struct {
	cfg Config

	mu   sync.Mutex
	subs map[string]Subscription
	// out queues requests for the current stream; nil while disconnected.
	out *queue.Queue[*chandlerv1.ConfigRequest]
	// lastErr is why the client is disconnected.
	lastErr error
}

// New returns a Client; call Run to connect it.
func New(cfg Config) *Client {
	return &Client{
		cfg:     cfg,
		subs:    make(map[string]Subscription),
		lastErr: errors.New("not connected yet"),
	}
}

// Subscribe registers a subscription. It is sent at once when the client is
// connected, and on every reconnect.
func (c *Client) Subscribe(s Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[s.ID] = s
	if c.out != nil {
		c.out.Push(subscribeRequest(s))
	}
}

// Unsubscribe ends a subscription. Unknown identifiers are ignored.
func (c *Client) Unsubscribe(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subs[id]; !ok {
		return
	}
	delete(c.subs, id)
	if c.out != nil {
		c.out.Push(chandlerv1.ConfigRequest_builder{
			Unsubscribe: chandlerv1.Unsubscribe_builder{SubscriptionId: proto.String(id)}.Build(),
		}.Build())
	}
}

// Report relays what an application said about a version. It is dropped when
// the client is not connected: the next Subscribe carries the loaded version,
// and a rejected version is refused by the driver itself if it comes again.
func (c *Client) Report(report *chandlerv1.AppReport) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.out != nil {
		c.out.Push(chandlerv1.ConfigRequest_builder{AppReport: report}.Build())
	}
}

// Err returns why the client is not connected, or nil when it is.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.out != nil {
		return nil
	}
	return c.lastErr
}

func subscribeRequest(s Subscription) *chandlerv1.ConfigRequest {
	subscribe := chandlerv1.Subscribe_builder{
		SubscriptionId: proto.String(s.ID),
		Uri:            proto.String(s.URI),
		Workload:       s.Workload,
	}
	applied := s.Handler.Applied()
	if applied != "" {
		subscribe.AppliedVersion = proto.String(applied)
	}
	for _, version := range s.Handler.Cached() {
		if version != applied {
			subscribe.CachedVersions = append(subscribe.CachedVersions, version)
		}
	}
	if s.AppAck {
		subscribe.AppAck = proto.Bool(true)
		if loaded := s.Handler.Loaded(); loaded != "" {
			subscribe.LoadedVersion = proto.String(loaded)
		}
	}
	return chandlerv1.ConfigRequest_builder{Subscribe: subscribe.Build()}.Build()
}

// Run keeps a stream to the server open until ctx ends, reconnecting with
// jittered backoff.
func (c *Client) Run(ctx context.Context) {
	log := c.cfg.Log.With("server", c.cfg.Target)
	// Plaintext: transport security between components is not implemented yet.
	conn, err := grpc.NewClient(c.cfg.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("invalid config server target", "error", err)
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return
	}
	defer conn.Close()
	client := chandlerv1.NewConfigDiscoveryServiceClient(conn)

	backoff := reconnectBase
	for ctx.Err() == nil {
		established, err := c.stream(ctx, client, log)
		if ctx.Err() != nil {
			return
		}
		if established {
			backoff = reconnectBase
		}
		log.Warn("config server stream ended; reconnecting", "error", err, "in", backoff.String())
		// Full jitter, so the drivers of a cluster do not reconnect in step.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(rand.Int64N(int64(backoff)) + 1)):
		}
		backoff = min(backoff*2, reconnectMax)
	}
}

// stream runs one stream until it fails. It reports whether the handshake
// completed.
func (c *Client) stream(ctx context.Context, client chandlerv1.ConfigDiscoveryServiceClient, log *slog.Logger) (established bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		c.mu.Lock()
		c.out = nil
		c.lastErr = fmt.Errorf("config server %s is unreachable: %w", c.cfg.Target, err)
		c.mu.Unlock()
	}()

	stream, err := client.StreamConfigs(ctx)
	if err != nil {
		return false, err
	}
	err = stream.Send(chandlerv1.ConfigRequest_builder{
		Hello: chandlerv1.NodeHello_builder{
			NodeId:        proto.String(c.cfg.NodeID),
			DriverVersion: proto.String(c.cfg.Version),
		}.Build(),
	}.Build())
	if err != nil {
		return false, err
	}
	first, err := stream.Recv()
	if err != nil {
		return false, err
	}
	if !first.HasHello() {
		return false, errors.New("the server's first message is not a ServerHello")
	}

	// A reconnect is a full resync: everything is subscribed again, each
	// with the version it already has.
	out := queue.New[*chandlerv1.ConfigRequest]()
	c.mu.Lock()
	c.out = out
	for _, s := range c.subs {
		out.Push(subscribeRequest(s))
	}
	n := len(c.subs)
	c.mu.Unlock()
	log.Info("connected to the config server", "server_id", first.GetHello().GetServerId(), "subscriptions", n)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-out.Wake():
				for _, msg := range out.Drain() {
					if err := stream.Send(msg); err != nil {
						cancel()
						return
					}
				}
			}
		}
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return true, err
		}
		switch {
		case msg.HasUpdate():
			c.apply(out, msg.GetUpdate(), log)
		case msg.HasError():
			if s, ok := c.subscription(msg.GetError().GetSubscriptionId()); ok {
				s.Handler.Fail(msg.GetError().GetStatus())
			}
		}
	}
}

func (c *Client) subscription(id string) (Subscription, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.subs[id]
	return s, ok
}

// apply hands an update to its subscription and answers the server: ACK when
// it was written, NACK with the reason when it was not.
func (c *Client) apply(out *queue.Queue[*chandlerv1.ConfigRequest], update *chandlerv1.ConfigUpdate, log *slog.Logger) {
	id := update.GetSubscriptionId()
	s, ok := c.subscription(id)
	if !ok {
		return // unsubscribed while the update was on its way
	}
	err := s.Handler.Apply(update)
	if err == nil {
		log.Info("wrote a version", "subscription", id, "uri", s.URI, "version", update.GetVersion())
		out.Push(chandlerv1.ConfigRequest_builder{
			Ack: chandlerv1.Ack_builder{
				SubscriptionId: proto.String(id),
				Nonce:          proto.String(update.GetNonce()),
				Version:        proto.String(update.GetVersion()),
			}.Build(),
		}.Build())
		return
	}

	st, _ := grpcstatus.FromError(err)
	code := st.Code()
	if code == codes.Unknown {
		code = codes.Internal
	}
	log.Error("could not write a version; the previous one stays", "subscription", id, "uri", s.URI,
		"version", update.GetVersion(), "code", code.String(), "error", st.Message())
	nack := chandlerv1.Nack_builder{
		SubscriptionId: proto.String(id),
		Nonce:          proto.String(update.GetNonce()),
		Version:        proto.String(update.GetVersion()),
		ErrorDetail:    &status.Status{Code: int32(code), Message: st.Message()},
	}
	if applied := s.Handler.Applied(); applied != "" {
		nack.AppliedVersion = proto.String(applied)
	}
	out.Push(chandlerv1.ConfigRequest_builder{Nack: nack.Build()}.Build())
}
