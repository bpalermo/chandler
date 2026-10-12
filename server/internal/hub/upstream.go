package hub

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/queue"
)

const (
	reconnectBase = time.Second
	reconnectMax  = 30 * time.Second
)

// upstream is one registered provider and the stream to it. Its fields are
// guarded by Hub.mu.
type upstream struct {
	hub    *Hub
	name   string
	target string

	// out queues requests for the current stream; nil while disconnected.
	out *queue.Queue[*chandlerv1.ProviderRequest]
	// watches maps the watch IDs of the current stream to their resources.
	watches map[string]*resource
}

// AddProvider registers a provider under name, the scheme of the URIs it
// serves, reachable at the gRPC target. Call it before Run.
func (h *Hub) AddProvider(name, target string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.upstreams[name]; ok {
		return fmt.Errorf("provider %q is registered twice", name)
	}
	h.upstreams[name] = &upstream{hub: h, name: name, target: target, watches: make(map[string]*resource)}
	return nil
}

// Run holds a stream to every registered provider until ctx ends.
func (h *Hub) Run(ctx context.Context) {
	h.mu.Lock()
	ups := make([]*upstream, 0, len(h.upstreams))
	for _, up := range h.upstreams {
		ups = append(ups, up)
	}
	h.mu.Unlock()

	done := make(chan struct{})
	for _, up := range ups {
		go func() {
			up.run(ctx)
			done <- struct{}{}
		}()
	}
	for range ups {
		<-done
	}
}

func (u *upstream) connected() bool { return u.out != nil }

func (u *upstream) unavailable() *status.Status {
	return &status.Status{
		Code:    int32(codes.Unavailable),
		Message: fmt.Sprintf("provider %s is not connected and no version is held", u.name),
	}
}

// watchLocked starts a watch for r on the current stream.
func (u *upstream) watchLocked(r *resource) {
	u.hub.watchSeq++
	r.watchID = strconv.FormatUint(u.hub.watchSeq, 10)
	r.failure = nil
	u.watches[r.watchID] = r
	watch := chandlerv1.Watch_builder{
		WatchId: proto.String(r.watchID),
		Uri:     proto.String(r.uri),
	}
	if r.version != "" {
		watch.KnownRevision = proto.String(r.revision)
	}
	u.out.Push(chandlerv1.ProviderRequest_builder{Watch: watch.Build()}.Build())
}

func (u *upstream) unwatchLocked(r *resource) {
	if r.watchID == "" {
		return
	}
	delete(u.watches, r.watchID)
	if u.connected() {
		u.out.Push(chandlerv1.ProviderRequest_builder{
			Unwatch: chandlerv1.Unwatch_builder{WatchId: proto.String(r.watchID)}.Build(),
		}.Build())
	}
	r.watchID = ""
}

// run keeps a stream to the provider open, reconnecting with jittered backoff.
func (u *upstream) run(ctx context.Context) {
	log := u.hub.log.With("provider", u.name, "target", u.target)
	// Plaintext: transport security between components is not implemented yet.
	conn, err := grpc.NewClient(u.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("invalid provider target", "error", err)
		return
	}
	defer conn.Close()
	client := chandlerv1.NewConfigProviderClient(conn)

	backoff := reconnectBase
	for ctx.Err() == nil {
		established, err := u.stream(ctx, client)
		if ctx.Err() != nil {
			return
		}
		if established {
			backoff = reconnectBase
		}
		log.Warn("provider stream ended; reconnecting", "error", err, "in", backoff.String())
		// Full jitter, so replicas do not reconnect in step.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(rand.Int64N(int64(backoff)) + 1)):
		}
		backoff = min(backoff*2, reconnectMax)
	}
}

// stream runs one stream to the provider until it fails. It reports whether
// the handshake completed.
func (u *upstream) stream(ctx context.Context, client chandlerv1.ConfigProviderClient) (bool, error) {
	h := u.hub
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.StreamConfigs(ctx)
	if err != nil {
		return false, err
	}
	err = stream.Send(chandlerv1.ProviderRequest_builder{
		Hello: chandlerv1.WatcherHello_builder{
			ServerId:       proto.String(h.id),
			MaxBundleBytes: proto.Uint64(h.limits.MaxBytes),
			MaxBundleFiles: proto.Uint32(uint32(h.limits.MaxFiles)),
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
		return false, fmt.Errorf("the provider's first message is not a ProviderHello")
	}
	// The scheme routes subscriptions, so a provider answering under another
	// name is a misconfiguration, not something to serve from.
	if got := first.GetHello().GetName(); got != u.name {
		return false, fmt.Errorf("registered as %q but the provider calls itself %q", u.name, got)
	}
	h.log.Info("provider connected", "provider", u.name)

	out := queue.New[*chandlerv1.ProviderRequest]()
	h.mu.Lock()
	u.out = out
	for _, r := range h.resources {
		if r.up == u {
			u.watchLocked(r)
		}
	}
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		u.out = nil
		u.watches = make(map[string]*resource)
		for _, r := range h.resources {
			if r.up != u {
				continue
			}
			r.watchID = ""
			// A resource with a version keeps serving it; one without has
			// nothing to give.
			if r.version == "" {
				r.failure = u.unavailable()
				h.answerPendingLocked(r)
			}
		}
	}()

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
		h.fromProvider(u, msg)
	}
}

// fromProvider handles one message of a provider stream.
func (h *Hub) fromProvider(u *upstream, msg *chandlerv1.ProviderResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch {
	case msg.HasUpdate():
		update := msg.GetUpdate()
		r, ok := u.watches[update.GetWatchId()]
		if !ok {
			return // a watch that has ended
		}
		h.updateLocked(u, r, update)
	case msg.HasError():
		r, ok := u.watches[msg.GetError().GetWatchId()]
		if !ok {
			return
		}
		st := msg.GetError().GetStatus()
		h.log.Warn("provider cannot serve a watch", "provider", u.name, "uri", r.uri,
			"code", codes.Code(st.GetCode()).String(), "reason", st.GetMessage())
		if r.version == "" {
			r.failure = &status.Status{Code: st.GetCode(), Message: st.GetMessage()}
			h.answerPendingLocked(r)
		}
	}
}

func (h *Hub) updateLocked(u *upstream, r *resource, update *chandlerv1.ResourceUpdate) {
	log := h.log.With("provider", u.name, "uri", r.uri, "revision", update.GetRevision())
	ack := func() {
		u.out.Push(chandlerv1.ProviderRequest_builder{
			Ack: chandlerv1.ProviderAck_builder{
				WatchId:  proto.String(update.GetWatchId()),
				Nonce:    proto.String(update.GetNonce()),
				Revision: proto.String(update.GetRevision()),
			}.Build(),
		}.Build())
	}

	switch {
	case update.HasBundle():
		if code, err := h.accept(update); err != nil {
			log.Error("refused a bundle from the provider", "code", code.String(), "error", err)
			u.out.Push(chandlerv1.ProviderRequest_builder{
				Nack: chandlerv1.ProviderNack_builder{
					WatchId:     proto.String(update.GetWatchId()),
					Nonce:       proto.String(update.GetNonce()),
					Revision:    proto.String(update.GetRevision()),
					ErrorDetail: &status.Status{Code: int32(code), Message: err.Error()},
				}.Build(),
			}.Build())
			// The last accepted version, if any, keeps being served.
			if r.version == "" {
				r.failure = &status.Status{
					Code:    int32(code),
					Message: fmt.Sprintf("provider %s emitted a bundle that cannot be delivered: %v", u.name, err),
				}
				h.answerPendingLocked(r)
			}
			return
		}
		r.bundle = update.GetBundle()
		r.version = bundle.Version(r.bundle)
		r.revision, r.cacheKey, r.failure = update.GetRevision(), update.GetCacheKey(), nil
		ack()
		log.Info("accepted a revision", "version", r.version, "subscriptions", len(r.subs))
		for sub := range r.subs {
			h.deliverLocked(sub, false)
		}
	case update.HasNotModified():
		ack()
	case update.HasDeleted():
		ack()
		if r.version != "" {
			// Config is never withdrawn from a pod: the last version stays.
			log.Warn("resource was deleted at the origin; keeping the last version", "version", r.version)
			return
		}
		r.failure = &status.Status{Code: int32(codes.NotFound), Message: "the resource was deleted at the origin"}
		h.answerPendingLocked(r)
	}
}
