package hub

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/queue"
	"github.com/bpalermo/chandler/common/rollout"
	"github.com/bpalermo/chandler/common/transport"
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
	// generation counts the streams, so that what was told on an earlier one
	// is told again.
	generation uint64
	// watches maps the watch IDs of the current stream to their resources.
	watches map[string]*resource
	// identities is what the provider's certificate must carry.
	identities []string
	// varyOn is the subscriber attributes the provider varies content on, as
	// it last declared them. Remembered across restarts, since it decides
	// which resource a subscription belongs to.
	varyOn []string
	// defaultCacheControl applies to updates that carry none.
	defaultCacheControl *chandlerv1.CacheControl
}

// project returns the values of the provider's vary attributes for one
// subscriber. An attribute with no value is absent.
func (u *upstream) project(w *chandlerv1.Workload, cluster string) map[string]string {
	if len(u.varyOn) == 0 {
		return nil
	}
	attributes := make(map[string]string)
	for _, name := range u.varyOn {
		value := ""
		switch name {
		case "cluster":
			value = cluster
		case "namespace":
			value = w.GetNamespace()
		case "service_account":
			value = w.GetServiceAccount()
		}
		if value != "" {
			attributes[name] = value
		}
	}
	return attributes
}

// AddProvider registers a provider under name, the scheme of the URIs it
// serves, reachable at the gRPC target and carrying one of the identities.
// Call it before Run.
func (h *Hub) AddProvider(name, target string, identities ...string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	up := h.upstreams[name]
	if up == nil {
		// Possibly known from the cache directory, with its vary attributes.
		up = &upstream{hub: h, name: name}
		h.upstreams[name] = up
	}
	if up.target != "" {
		return fmt.Errorf("provider %q is registered twice", name)
	}
	up.target, up.identities, up.watches = target, identities, make(map[string]*resource)
	return nil
}

// Run holds a stream to every registered provider until ctx ends.
func (h *Hub) Run(ctx context.Context) {
	h.mu.Lock()
	ups := make([]*upstream, 0, len(h.upstreams))
	for name, up := range h.upstreams {
		if up.target == "" {
			// Remembered from the cache directory but no longer configured.
			delete(h.upstreams, name)
			continue
		}
		ups = append(ups, up)
	}
	h.mu.Unlock()

	go h.keep(ctx)

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
	r.failure, r.answered, r.live = nil, false, false
	u.watches[r.watchID] = r
	watch := chandlerv1.Watch_builder{
		WatchId:    proto.String(r.watchID),
		Uri:        proto.String(r.uri),
		Attributes: r.attributes,
	}
	if r.entry != nil {
		watch.KnownRevision = proto.String(r.entry.newest().Revision)
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
	r.watchID, r.live, r.answered = "", false, false
}

// run keeps a stream to the provider open, reconnecting with jittered backoff.
func (u *upstream) run(ctx context.Context) {
	log := u.hub.log.With("provider", u.name, "target", u.target)
	conn, err := grpc.NewClient(u.target, grpc.WithTransportCredentials(u.hub.dial(u.identities)), transport.ClientKeepalive())
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
	varyOn := append([]string(nil), first.GetHello().GetVaryOn()...)
	sort.Strings(varyOn)
	for _, name := range varyOn {
		if !varyAllowed[name] {
			return false, fmt.Errorf("the provider varies on %q, which is not an attribute a provider may vary on", name)
		}
	}
	h.log.Info("provider connected", "provider", u.name, "vary_on", strings.Join(varyOn, ","))

	out := queue.New[*chandlerv1.ProviderRequest]()
	h.mu.Lock()
	u.out = out
	u.generation++
	u.defaultCacheControl = first.GetHello().GetDefaultCacheControl()
	if !slices.Equal(varyOn, u.varyOn) {
		// Which resource a subscription belongs to has changed: sort every
		// subscription of this provider again.
		u.varyOn = varyOn
		h.regroupLocked(u)
		h.saveLocked()
	}
	for _, r := range h.resources {
		if r.up == u && r.delayed == nil {
			u.watchLocked(r)
		}
	}
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		u.out = nil
		u.watches = make(map[string]*resource)
		now := h.now()
		for _, r := range h.resources {
			if r.up != u {
				continue
			}
			if r.live && r.entry != nil {
				r.entry.lastRefreshed = now
			}
			r.watchID, r.live, r.answered = "", false, false
			if r.entry == nil && r.failure == nil {
				r.failure = u.unavailable()
			}
			// Whoever still waits for an initial answer gets it now: the
			// held version, marked stale, or the reason there is none.
			h.serveAllLocked(r)
		}
		h.saveLocked()
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

// regroupLocked re-resolves every subscription of a provider to its resource,
// after the provider's vary attributes changed.
func (h *Hub) regroupLocked(u *upstream) {
	var subs []*subscription
	for _, r := range h.resources {
		if r.up == u {
			for sub := range r.subs {
				subs = append(subs, sub)
			}
		}
	}
	for _, sub := range subs {
		h.dropLocked(sub)
	}
	for _, sub := range subs {
		sub.res = nil
		h.attachLocked(sub)
	}
}

// shareOf returns the share of subscribers an update releases its revision
// to, in basis points.
func shareOf(update *chandlerv1.ResourceUpdate) uint32 {
	if !update.HasRollout() {
		return rollout.Everyone
	}
	return min(update.GetRollout().GetBasisPoints(), rollout.Everyone)
}

// releaseLocked makes v the stable version, ending any rollout.
func (e *entry) releaseLocked(v held) {
	versions := []held{v}
	for _, old := range e.versions {
		if old.Version != v.Version && len(versions) < VersionsPerKey {
			versions = append(versions, old)
		}
	}
	e.versions, e.candidate, e.share = versions, nil, 0
}

// pruneLocked drops the content of versions the entry no longer holds.
func (e *entry) pruneLocked() {
	keep := make(map[string]bool)
	for _, v := range e.all() {
		keep[v.Version] = true
	}
	for version := range e.bundles {
		if !keep[version] {
			delete(e.bundles, version)
		}
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
		h.failLocked(r, &status.Status{Code: st.GetCode(), Message: st.GetMessage()})
	}
}

// failLocked records that the provider cannot serve a resource's watch. A
// held version keeps being served, as stale.
func (h *Hub) failLocked(r *resource, st *status.Status) {
	if r.live && r.entry != nil {
		r.entry.lastRefreshed = h.now()
	}
	r.failure, r.answered, r.live = st, true, false
	switch codes.Code(st.GetCode()) {
	case codes.NotFound, codes.PermissionDenied:
		// Remembered briefly, so a pod that keeps asking does not make the
		// server keep asking the provider.
		if r.entry == nil && (len(h.negative) < maxNegative || h.negative[r.id].status != nil) {
			h.negative[r.id] = negative{status: st, until: h.now().Add(h.negativeTTL)}
		}
	}
	h.serveAllLocked(r)
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
	refuse := func(code codes.Code, err error) {
		log.Error("refused a bundle from the provider", "code", code.String(), "error", err)
		h.metrics.refusedBundles.Add(context.Background(), 1, metric.WithAttributes(
			attribute.String("provider", u.name), attribute.String("code", code.String())))
		u.out.Push(chandlerv1.ProviderRequest_builder{
			Nack: chandlerv1.ProviderNack_builder{
				WatchId:     proto.String(update.GetWatchId()),
				Nonce:       proto.String(update.GetNonce()),
				Revision:    proto.String(update.GetRevision()),
				ErrorDetail: &status.Status{Code: int32(code), Message: err.Error()},
			}.Build(),
		}.Build())
		// The last accepted version, if any, keeps being served.
		h.failLocked(r, &status.Status{
			Code:    int32(code),
			Message: fmt.Sprintf("provider %s emitted a bundle that cannot be delivered: %v", u.name, err),
		})
	}
	now := h.now()

	switch {
	case update.HasBundle():
		if code, err := h.accept(update); err != nil {
			refuse(code, err)
			return
		}
		b := update.GetBundle()
		key := entryKey{u.name, update.GetCacheKey()}
		version, size := bundle.Version(b), bundle.Size(b)
		if err := h.admit(r, key, size); err != nil {
			refuse(codes.ResourceExhausted, err)
			return
		}
		cc := update.GetCacheControl()
		if cc == nil {
			cc = u.defaultCacheControl
		}
		e := h.entries[key]
		if e == nil || e.bundles[version] == nil {
			// no_store content is never written to disk.
			if h.store != nil && !cc.GetNoStore() {
				if err := h.store.Put(version, b); err != nil {
					refuse(codes.Internal, err)
					return
				}
			}
		}
		if e == nil {
			e = &entry{key: key, bundles: make(map[string]*chandlerv1.Bundle), refs: make(map[*resource]struct{})}
			h.entries[key] = e
		}
		newest := held{Version: version, Revision: update.GetRevision(), Bytes: size}
		share := shareOf(update)
		switch {
		case len(e.versions) > 0 && e.current().Version == version:
			// The content everyone already has, perhaps under a new
			// revision: not a rollout, and the end of one in progress.
			e.versions[0], e.candidate, e.share = newest, nil, 0
		case len(e.versions) == 0 || share >= rollout.Everyone:
			// Released to everyone. A resource's first version always is:
			// there is nothing else to give the rest.
			e.releaseLocked(newest)
		case e.candidate != nil && e.candidate.Version == version:
			// More of the same rollout. The share never shrinks: a pod that
			// has the version keeps it.
			e.candidate, e.share = &newest, max(e.share, share)
		default:
			// A new rollout, replacing any that was in progress.
			e.candidate, e.share = &newest, share
		}
		e.bundles[version] = b
		e.pruneLocked()
		e.cacheControl, e.lastRefreshed, e.lastReferenced, e.orphaned = cc, now, now, false

		h.bindLocked(r, e)
		h.aliases[r.id] = alias{Provider: u.name, CacheKey: key.cacheKey, URI: r.uri, Attributes: r.attributes}
		delete(h.negative, r.id)
		r.failure, r.answered, r.live = nil, true, true
		ack()
		h.saveLocked()
		log.Info("accepted a revision", "version", version, "cache_key", key.cacheKey,
			"stable", e.current().Version, "basis_points", e.share)
		// Every resource that resolves to the key gets the version.
		for other := range e.refs {
			h.serveAllLocked(other)
		}
	case update.HasNotModified():
		ack()
		if r.entry == nil {
			return
		}
		e := r.entry
		e.lastRefreshed, e.orphaned = now, false
		if cc := update.GetCacheControl(); cc != nil {
			e.cacheControl = cc
		}
		switch share := shareOf(update); {
		case e.candidate != nil && e.candidate.Revision == update.GetRevision():
			if share >= rollout.Everyone {
				log.Info("rollout finished", "version", e.candidate.Version)
				e.releaseLocked(*e.candidate)
				e.pruneLocked()
			} else if share > e.share {
				log.Info("rollout advanced", "version", e.candidate.Version, "basis_points", share)
				e.share = share
			}
		case e.candidate != nil && e.current().Revision == update.GetRevision():
			// The provider's newest revision is the stable one again: the
			// candidate was taken back.
			log.Info("rollout taken back", "version", e.candidate.Version)
			e.candidate, e.share = nil, 0
			e.pruneLocked()
		}
		r.failure, r.answered, r.live = nil, true, true
		h.saveLocked()
		// Placement may have changed for every resource of the key.
		for other := range e.refs {
			h.serveAllLocked(other)
		}
	case update.HasDeleted():
		ack()
		if r.entry == nil {
			h.failLocked(r, &status.Status{Code: int32(codes.NotFound), Message: "the resource was deleted at the origin"})
			return
		}
		// Config is never withdrawn: the last version stays and keeps being
		// served, to running pods and to new ones.
		log.Warn("resource was deleted at the origin; keeping the last version", "version", r.entry.current().Version)
		if r.live {
			r.entry.lastRefreshed = now
		}
		r.entry.orphaned = true
		r.failure, r.answered, r.live = nil, true, false
		h.saveLocked()
		h.serveAllLocked(r)
	}
}
