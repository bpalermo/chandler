// Package hub is the config server's core: it joins the subscriptions of CSI
// drivers to watches on providers.
//
// Every subscription for the same canonical URI shares one resource, and a
// resource holds one watch on the provider its scheme names. What a provider
// emits is checked as an envelope (size, file count, paths), given a version
// and pushed to the resource's subscriptions.
//
// This is the minimal server. Resources live in memory and are dropped with
// their last subscription; there is no persistent cache, no peer sync and no
// staged rollout, so every subscription gets the provider's newest revision.
package hub

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/queue"
	"github.com/bpalermo/chandler/common/uri"
)

const (
	maxCacheKeyBytes = 256

	// maxResends bounds how often one version is sent again to a subscription
	// that reports DATA_LOSS for it, so a node that can never verify a bundle
	// does not loop.
	maxResends = 3

	retryBase = time.Second
	retryMax  = time.Minute
)

// Config configures a Hub.
type Config struct {
	// ServerID identifies this replica to drivers and providers.
	ServerID string
	// Limits bounds the bundles accepted from providers.
	Limits bundle.Limits
	Log    *slog.Logger
}

// Hub serves ConfigDiscoveryService and watches providers.
type Hub struct {
	chandlerv1.UnimplementedConfigDiscoveryServiceServer

	id     string
	limits bundle.Limits
	log    *slog.Logger

	// mu guards everything below, and the fields of every upstream, resource
	// and subscription. Nothing blocks while holding it: messages are queued,
	// never sent.
	mu        sync.Mutex
	upstreams map[string]*upstream
	resources map[string]*resource
	watchSeq  uint64
}

// New returns a Hub with no providers.
func New(cfg Config) *Hub {
	return &Hub{
		id:        cfg.ServerID,
		limits:    cfg.Limits,
		log:       cfg.Log,
		upstreams: make(map[string]*upstream),
		resources: make(map[string]*resource),
	}
}

// resource is one canonical URI and what the provider last said about it.
type resource struct {
	uri string
	up  *upstream
	// watchID is the provider watch of the current provider stream; "" while
	// the provider is not connected.
	watchID string

	// version is "" until the provider has delivered an acceptable bundle.
	version  string
	revision string
	cacheKey string
	bundle   *chandlerv1.Bundle

	// failure says why there is no version. nil while the answer to the
	// watch is still awaited.
	failure *status.Status

	subs map[*subscription]struct{}
}

// subscription is one source of one pod volume, on one driver stream.
type subscription struct {
	id   string
	sess *session
	res  *resource

	// answered is set once the initial answer was sent.
	answered bool
	// sent and nonce are the last update sent; acked is the last version the
	// driver reported written.
	sent, nonce, acked string
	// nodeHas is the versions the driver said it holds, which can be sent as
	// UseCached.
	nodeHas map[string]bool
	// attempts counts sends of the current version beyond the first.
	attempts int
	retry    *time.Timer
}

// session is one driver's stream.
type session struct {
	node  string
	out   *queue.Queue[*chandlerv1.ConfigResponse]
	subs  map[string]*subscription
	nonce uint64
}

func (s *session) nextNonce() string {
	s.nonce++
	return strconv.FormatUint(s.nonce, 10)
}

func (s *session) fail(id string, code codes.Code, msg string) {
	s.out.Push(chandlerv1.ConfigResponse_builder{
		Error: chandlerv1.SubscriptionError_builder{
			SubscriptionId: proto.String(id),
			Status:         &status.Status{Code: int32(code), Message: msg},
		}.Build(),
	}.Build())
}

// subscribe registers a subscription and sends its initial answer if one is
// already known.
func (h *Hub) subscribe(sess *session, msg *chandlerv1.Subscribe) {
	id := msg.GetSubscriptionId()

	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := sess.subs[id]; ok {
		h.dropLocked(old)
	}
	u, err := uri.Parse(msg.GetUri())
	if err != nil {
		sess.fail(id, codes.InvalidArgument, err.Error())
		return
	}
	up, ok := h.upstreams[u.Provider]
	if !ok {
		sess.fail(id, codes.NotFound, fmt.Sprintf("no provider is registered for %s://", u.Provider))
		return
	}

	canonical := u.String()
	r, ok := h.resources[canonical]
	if !ok {
		r = &resource{uri: canonical, up: up, subs: make(map[*subscription]struct{})}
		h.resources[canonical] = r
		if up.connected() {
			up.watchLocked(r)
		} else {
			r.failure = up.unavailable()
		}
	}

	sub := &subscription{id: id, sess: sess, res: r, nodeHas: make(map[string]bool)}
	if v := msg.GetAppliedVersion(); v != "" {
		sub.nodeHas[v] = true
	}
	for _, v := range msg.GetCachedVersions() {
		sub.nodeHas[v] = true
	}
	r.subs[sub] = struct{}{}
	sess.subs[id] = sub

	h.deliverLocked(sub, false)
	h.answerPendingLocked(r)
}

func (h *Hub) unsubscribe(sess *session, id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if sub, ok := sess.subs[id]; ok {
		h.dropLocked(sub)
	}
}

// closeSession drops everything a driver stream held.
func (h *Hub) closeSession(sess *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range sess.subs {
		h.dropLocked(sub)
	}
}

// dropLocked removes a subscription, and its resource with the last one.
func (h *Hub) dropLocked(sub *subscription) {
	if sub.retry != nil {
		sub.retry.Stop()
	}
	delete(sub.sess.subs, sub.id)
	r := sub.res
	delete(r.subs, sub)
	if len(r.subs) == 0 {
		r.up.unwatchLocked(r)
		delete(h.resources, r.uri)
	}
}

// deliverLocked sends the resource's version to a subscription that does not
// have it yet, or again when force is set.
func (h *Hub) deliverLocked(sub *subscription, force bool) {
	r := sub.res
	if r.version == "" || (sub.sent == r.version && !force) {
		return
	}
	if sub.sent != r.version {
		sub.attempts = 0
	}
	nonce := sub.sess.nextNonce()
	update := chandlerv1.ConfigUpdate_builder{
		SubscriptionId: proto.String(sub.id),
		Nonce:          proto.String(nonce),
		Version:        proto.String(r.version),
		CacheKey:       proto.String(r.cacheKey),
	}
	if sub.nodeHas[r.version] {
		update.UseCached = &chandlerv1.UseCached{}
	} else {
		update.Bundle = r.bundle
	}
	sub.sent, sub.nonce, sub.answered = r.version, nonce, true
	sub.sess.out.Push(chandlerv1.ConfigResponse_builder{Update: update.Build()}.Build())
}

// answerPendingLocked gives the resource's failure to the subscriptions still
// waiting for their initial answer.
func (h *Hub) answerPendingLocked(r *resource) {
	if r.version != "" || r.failure == nil {
		return
	}
	for sub := range r.subs {
		if !sub.answered {
			sub.answered = true
			sub.sess.fail(sub.id, codes.Code(r.failure.GetCode()), r.failure.GetMessage())
		}
	}
}

// ack records that the driver wrote a version.
func (h *Hub) ack(sess *session, msg *chandlerv1.Ack) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := sess.subs[msg.GetSubscriptionId()]
	if !ok || msg.GetNonce() != sub.nonce {
		return // superseded: only the newest update counts
	}
	sub.acked, sub.attempts = msg.GetVersion(), 0
	sub.nodeHas[sub.acked] = true
}

// nack reacts to a failed write according to its code.
func (h *Hub) nack(sess *session, msg *chandlerv1.Nack) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := sess.subs[msg.GetSubscriptionId()]
	if !ok || msg.GetNonce() != sub.nonce {
		return
	}
	version := msg.GetVersion()
	code := codes.Code(msg.GetErrorDetail().GetCode())
	log := h.log.With("node", sess.node, "subscription", sub.id, "uri", sub.res.uri,
		"version", version, "code", code.String(), "reason", msg.GetErrorDetail().GetMessage())

	switch code {
	case codes.DataLoss:
		// The node could not verify the bytes or lacks a version it was
		// told to reuse: send the bundle itself.
		delete(sub.nodeHas, version)
		if sub.attempts >= maxResends {
			log.Error("driver cannot verify the bundle; giving up on this version")
			return
		}
		sub.attempts++
		log.Warn("driver reported data loss; resending the bundle")
		h.deliverLocked(sub, true)
	case codes.ResourceExhausted, codes.Internal:
		// A node-side failure that may pass: try again later.
		delay := min(retryBase<<min(sub.attempts, 10), retryMax)
		sub.attempts++
		log.Warn("driver could not write the bundle; will retry", "delay", delay.String())
		if sub.retry != nil {
			sub.retry.Stop()
		}
		sub.retry = time.AfterFunc(delay, func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if sess.subs[sub.id] == sub && sub.res.version == version && sub.acked != version {
				h.deliverLocked(sub, true)
			}
		})
	default:
		// The bundle can never be written to that volume: not resent.
		log.Error("driver refused the bundle")
	}
}

// accept checks the envelope of a provider's update.
func (h *Hub) accept(u *chandlerv1.ResourceUpdate) (codes.Code, error) {
	switch {
	case u.GetCacheKey() == "":
		return codes.InvalidArgument, errors.New("update has no cache_key")
	case len(u.GetCacheKey()) > maxCacheKeyBytes:
		return codes.InvalidArgument, fmt.Errorf("cache_key is longer than %d bytes", maxCacheKeyBytes)
	case u.GetRevision() == "":
		return codes.InvalidArgument, errors.New("update has no revision")
	}
	err := bundle.Validate(u.GetBundle(), h.limits)
	switch {
	case err == nil:
		return codes.OK, nil
	case errors.Is(err, bundle.ErrTooLarge), errors.Is(err, bundle.ErrTooManyFiles):
		return codes.ResourceExhausted, err
	default:
		return codes.InvalidArgument, err
	}
}
