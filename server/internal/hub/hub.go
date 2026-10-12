// Package hub is the config server's core: it joins the subscriptions of CSI
// drivers to watches on providers, and keeps what the providers emit.
//
// A subscription names a URI. The URI, with the subscriber attributes its
// provider declared in vary_on, identifies a resource, and every subscription
// for the same resource shares one watch. The provider answers a watch with a
// cache key, and what the server keeps is keyed by that: several resources
// may resolve to one key and then share one entry.
//
// An entry outlives its subscriptions and its provider's stream. While the
// provider is unreachable the entry keeps being served, marked stale, until
// its CacheControl says it has expired; an entry nothing refers to is dropped
// after a grace period. Entries and their bundles are written to the cache
// directory, so a restarted container starts with what it had.
//
// A provider may release a new version to a share of the subscribers first.
// The entry then holds a stable version, which everyone had, and a candidate
// with its share; which of the two a pod gets is a pure function of the
// candidate's version, the pod's UID and the share (common/rollout). The
// provider owns the rollout: it raises the share, holds it, or takes the
// version back. The server only places pods and reports how they fare.
package hub

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/cas"
	"github.com/bpalermo/chandler/common/queue"
	"github.com/bpalermo/chandler/common/rollout"
	"github.com/bpalermo/chandler/common/uri"
)

const (
	maxCacheKeyBytes = 256

	// VersionsPerKey is how many released versions an entry holds: the
	// stable one and the one before it. A candidate being rolled out is a
	// third.
	VersionsPerKey = 2

	// maxResends bounds how often one version is sent again to a subscription
	// that reports DATA_LOSS for it, so a node that can never verify a bundle
	// does not loop.
	maxResends = 3

	retryBase = time.Second
	retryMax  = time.Minute

	// maxNegative bounds the remembered failures.
	maxNegative = 4096
)

// varyAllowed is the subscriber attributes a provider may vary content on.
// They are low-cardinality by nature; nothing that identifies a single pod is
// here, so a pod can never mint a cache entry of its own.
var varyAllowed = map[string]bool{
	"cluster":         true,
	"namespace":       true,
	"service_account": true,
}

// Quotas bounds the cache. Reaching one fails loudly, toward the provider or
// the subscriber; nothing in use is evicted to make room.
type Quotas struct {
	// MaxKeysPerProvider is the most cache keys one provider may issue.
	MaxKeysPerProvider int
	// MaxBytesPerProvider is the most content held for one provider,
	// counting the current version of each key.
	MaxBytesPerProvider uint64
	// MaxVariantsPerURI is the most cache keys one canonical URI may
	// resolve to across subscriber attributes.
	MaxVariantsPerURI int
	// MaxURIsPerNamespace is the most distinct URIs the pods of one
	// namespace may be subscribed to.
	MaxURIsPerNamespace int
}

// DefaultQuotas returns the quotas applied when none are configured.
func DefaultQuotas() Quotas {
	return Quotas{
		MaxKeysPerProvider:  10000,
		MaxBytesPerProvider: 1 << 30,
		MaxVariantsPerURI:   16,
		MaxURIsPerNamespace: 1000,
	}
}

// Config configures a Hub.
type Config struct {
	// ServerID identifies this replica to drivers and providers.
	ServerID string
	// Limits bounds the bundles accepted from providers.
	Limits bundle.Limits
	Quotas Quotas
	// CacheDir is where entries and bundles are kept. Empty keeps them in
	// memory only.
	CacheDir string
	// Grace is how long an entry nothing refers to is kept.
	Grace time.Duration
	// NegativeTTL is how long a NOT_FOUND or PERMISSION_DENIED answer is
	// remembered, so a crash-looping pod does not hammer a provider.
	NegativeTTL time.Duration
	Log         *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Hub serves ConfigDiscoveryService and watches providers.
type Hub struct {
	chandlerv1.UnimplementedConfigDiscoveryServiceServer
	chandlerv1.UnimplementedPeerSyncServer

	id          string
	limits      bundle.Limits
	quotas      Quotas
	grace       time.Duration
	negativeTTL time.Duration
	log         *slog.Logger
	now         func() time.Time
	cacheDir    string
	store       *cas.Store

	// mu guards everything below, and the fields of every upstream, resource,
	// entry and subscription. Nothing blocks on the network while holding it:
	// messages are queued, never sent.
	mu        sync.Mutex
	upstreams map[string]*upstream
	// resources is the resources that have a subscription, by resource ID.
	resources map[string]*resource
	// entries is everything held, by provider and cache key.
	entries map[entryKey]*entry
	// aliases remembers which entry a resource ID resolved to.
	aliases map[string]alias
	// negative remembers recent failures by resource ID.
	negative map[string]negative
	// namespaces counts the subscriptions per namespace and URI.
	namespaces map[string]map[string]int
	watchSeq   uint64
}

// New returns a Hub with no providers, loading what the cache directory holds.
func New(cfg Config) (*Hub, error) {
	h := &Hub{
		id:          cfg.ServerID,
		limits:      cfg.Limits,
		quotas:      cfg.Quotas,
		grace:       cfg.Grace,
		negativeTTL: cfg.NegativeTTL,
		log:         cfg.Log,
		now:         cfg.Now,
		cacheDir:    cfg.CacheDir,
		upstreams:   make(map[string]*upstream),
		resources:   make(map[string]*resource),
		entries:     make(map[entryKey]*entry),
		aliases:     make(map[string]alias),
		negative:    make(map[string]negative),
		namespaces:  make(map[string]map[string]int),
	}
	if h.now == nil {
		h.now = time.Now
	}
	if h.quotas == (Quotas{}) {
		h.quotas = DefaultQuotas()
	}
	if h.cacheDir != "" {
		store, err := cas.Open(h.cacheDir)
		if err != nil {
			return nil, err
		}
		h.store = store
		if err := h.load(); err != nil {
			return nil, err
		}
	}
	return h, nil
}

type entryKey struct{ provider, cacheKey string }

type alias struct {
	Provider string `json:"provider"`
	CacheKey string `json:"cacheKey"`
	URI      string `json:"uri"`
	// Attributes is the vary attributes the alias holds for.
	Attributes map[string]string `json:"attributes,omitempty"`
}

type negative struct {
	status *status.Status
	until  time.Time
}

// held is one version an entry holds.
type held struct {
	Version  string `json:"version"`
	Revision string `json:"revision"`
	Bytes    uint64 `json:"bytes"`
}

// entry is what is held for one cache key of one provider.
type entry struct {
	key entryKey
	// versions is the versions released to everyone, newest first, at most
	// VersionsPerKey; never empty. The first is the stable version.
	versions []held
	// candidate is the version being rolled out, or nil.
	candidate *held
	// share is the share of subscribers that gets the candidate, in basis
	// points. It never shrinks for one candidate.
	share uint32
	// reported is the feedback last sent to the provider, and on which
	// provider stream.
	reported   feedback
	reportedOn uint64
	// bundles holds the content of the versions, by version.
	bundles      map[string]*chandlerv1.Bundle
	cacheControl *chandlerv1.CacheControl
	// lastRefreshed is when a provider watch for the key was last live;
	// freshness is measured from it.
	lastRefreshed time.Time
	// lastReferenced is when a subscription last used the key.
	lastReferenced time.Time
	// orphaned is set when the resource was deleted at the origin.
	orphaned bool
	// refs is the subscribed resources that resolve to this entry.
	refs map[*resource]struct{}
}

func (e *entry) current() held { return e.versions[0] }

// newest returns the version the provider last emitted: the candidate if one
// is rolling out, else the stable version.
func (e *entry) newest() held {
	if e.candidate != nil {
		return *e.candidate
	}
	return e.current()
}

// all returns every version the entry holds.
func (e *entry) all() []held {
	if e.candidate == nil {
		return e.versions
	}
	return append([]held{*e.candidate}, e.versions...)
}

// target returns the version a pod gets, and which side of the rollout that
// puts it on.
func (e *entry) target(podUID string) (held, chandlerv1.Track) {
	switch {
	case e.candidate == nil:
		return e.current(), chandlerv1.Track_TRACK_UNSPECIFIED
	case podUID != "" && rollout.Placed(e.candidate.Version, podUID, e.share):
		return *e.candidate, chandlerv1.Track_TRACK_CANDIDATE
	default:
		return e.current(), chandlerv1.Track_TRACK_STABLE
	}
}

// feedback is what a provider is told about one revision.
type feedback struct {
	revision                            string
	assigned, written, refused, failing uint32
}

// live reports whether a provider watch currently vouches for the entry.
func (e *entry) live() bool {
	for r := range e.refs {
		if r.live {
			return true
		}
	}
	return false
}

// resource is one URI as seen by a class of subscribers: the canonical URI
// and the vary attributes. It exists while it has a subscription.
type resource struct {
	id         string
	uri        string
	attributes map[string]string
	up         *upstream
	// watchID is the provider watch on the current provider stream; ""
	// while there is none.
	watchID string
	// answered is set once the provider answered the current watch.
	answered bool
	// live is set while the provider's answer says the entry is current.
	live bool
	// delayed is the timer that starts the watch after a remembered failure.
	delayed *time.Timer

	entry *entry
	// failure is the provider's reason for not serving the watch.
	failure *status.Status

	subs map[*subscription]struct{}
}

// subscription is one source of one pod volume, on one driver stream.
type subscription struct {
	id   string
	sess *session
	res  *resource
	// request is what the driver subscribed with.
	request *chandlerv1.Subscribe

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
	// refused is the version the driver said it can never write; failing is
	// the version it is failing to write for a reason on the node.
	refused, failing string
}

// session is one driver's stream.
type session struct {
	node    string
	cluster string
	out     *queue.Queue[*chandlerv1.ConfigResponse]
	subs    map[string]*subscription
	nonce   uint64
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
	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := sess.subs[msg.GetSubscriptionId()]; ok {
		h.dropLocked(old)
	}
	sub := &subscription{id: msg.GetSubscriptionId(), sess: sess, request: msg, nodeHas: make(map[string]bool)}
	if v := msg.GetAppliedVersion(); v != "" {
		sub.nodeHas[v] = true
	}
	for _, v := range msg.GetCachedVersions() {
		sub.nodeHas[v] = true
	}
	h.attachLocked(sub)
}

// attachLocked resolves a subscription to its resource, creating and watching
// the resource if it is the first.
func (h *Hub) attachLocked(sub *subscription) {
	sess, id := sub.sess, sub.id
	u, err := uri.Parse(sub.request.GetUri())
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
	namespace := sub.request.GetWorkload().GetNamespace()
	if h.namespaces[namespace][canonical] == 0 && len(h.namespaces[namespace]) >= h.quotas.MaxURIsPerNamespace {
		sess.fail(id, codes.ResourceExhausted, fmt.Sprintf(
			"namespace %q is already subscribed to %d distinct URIs, the limit", namespace, h.quotas.MaxURIsPerNamespace))
		return
	}

	attributes := up.project(sub.request.GetWorkload(), sess.cluster)
	rid := resourceID(canonical, attributes)
	r, ok := h.resources[rid]
	if !ok {
		r = &resource{id: rid, uri: canonical, attributes: attributes, up: up, subs: make(map[*subscription]struct{})}
		h.resources[rid] = r
		if a, ok := h.aliases[rid]; ok {
			if e := h.entries[entryKey{a.Provider, a.CacheKey}]; e != nil {
				h.bindLocked(r, e)
			}
		}
		h.startLocked(r)
	}

	if h.namespaces[namespace] == nil {
		h.namespaces[namespace] = make(map[string]int)
	}
	h.namespaces[namespace][canonical]++
	sub.res = r
	r.subs[sub] = struct{}{}
	sess.subs[id] = sub
	if r.entry != nil {
		r.entry.lastReferenced = h.now()
	}
	h.serveLocked(sub, false)
}

// startLocked begins watching a new resource, unless a failure remembered for
// it says not to ask the provider again yet.
func (h *Hub) startLocked(r *resource) {
	if n, ok := h.negative[r.id]; ok && r.entry == nil {
		if wait := n.until.Sub(h.now()); wait > 0 {
			r.failure, r.answered = n.status, true
			r.delayed = time.AfterFunc(wait, func() {
				h.mu.Lock()
				defer h.mu.Unlock()
				if h.resources[r.id] == r && r.watchID == "" && r.up.connected() {
					r.answered = false
					r.up.watchLocked(r)
				}
			})
			return
		}
		delete(h.negative, r.id)
	}
	if r.up.connected() {
		r.up.watchLocked(r)
	} else if r.entry == nil {
		r.failure = r.up.unavailable()
	}
}

// bindLocked makes e the entry of r.
func (h *Hub) bindLocked(r *resource, e *entry) {
	if r.entry == e {
		return
	}
	if r.entry != nil {
		delete(r.entry.refs, r)
	}
	r.entry = e
	e.refs[r] = struct{}{}
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

// dropLocked removes a subscription, and its resource with the last one. The
// resource's entry stays.
func (h *Hub) dropLocked(sub *subscription) {
	if sub.retry != nil {
		sub.retry.Stop()
	}
	delete(sub.sess.subs, sub.id)
	r := sub.res
	if r == nil {
		return
	}
	delete(r.subs, sub)

	namespace := sub.request.GetWorkload().GetNamespace()
	if uris := h.namespaces[namespace]; uris != nil {
		if uris[r.uri]--; uris[r.uri] <= 0 {
			delete(uris, r.uri)
		}
		if len(uris) == 0 {
			delete(h.namespaces, namespace)
		}
	}
	if len(r.subs) > 0 {
		return
	}
	if r.delayed != nil {
		r.delayed.Stop()
	}
	if e := r.entry; e != nil {
		now := h.now()
		e.lastReferenced = now
		if r.live {
			e.lastRefreshed = now
		}
		delete(e.refs, r)
		h.saveLocked()
	}
	r.up.unwatchLocked(r)
	delete(h.resources, r.id)
}

// expired reports whether an entry that no watch vouches for is too old to be
// given to a new subscription.
func (h *Hub) expired(e *entry) bool {
	cc := e.cacheControl
	if !cc.HasStaleIfError() {
		// Unlimited: for configuration an old version is almost always
		// better than none.
		return false
	}
	limit := cc.GetMaxAge().AsDuration() + cc.GetStaleIfError().AsDuration()
	return h.now().Sub(e.lastRefreshed) > limit
}

// serveLocked sends a subscription the newest version of its entry if it does
// not have it yet (or again when force is set), or the reason there is none
// as its initial answer.
func (h *Hub) serveLocked(sub *subscription, force bool) {
	r := sub.res
	e := r.entry
	if e == nil {
		if r.failure != nil && !sub.answered {
			sub.answered = true
			sub.sess.fail(sub.id, codes.Code(r.failure.GetCode()), r.failure.GetMessage())
		}
		return
	}

	stale := false
	if !r.live {
		if r.up.connected() && r.watchID != "" && !r.answered {
			return // the provider is about to say whether this is current
		}
		// Nothing vouches for the entry: the provider is unreachable, or
		// cannot serve the watch.
		stale = true
		if !sub.answered && h.expired(e) {
			sub.answered = true
			sub.sess.fail(sub.id, codes.Unavailable, fmt.Sprintf(
				"provider %s cannot confirm %s and the cached version has expired", r.up.name, r.uri))
			return
		}
	}

	target, track := e.target(sub.request.GetWorkload().GetPodUid())
	version := target.Version
	if sub.sent == version && !force {
		sub.answered = true
		return
	}
	if sub.sent != version {
		sub.attempts = 0
	}
	nonce := sub.sess.nextNonce()
	update := chandlerv1.ConfigUpdate_builder{
		SubscriptionId: proto.String(sub.id),
		Nonce:          proto.String(nonce),
		Version:        proto.String(version),
		CacheKey:       proto.String(e.key.cacheKey),
		CacheControl:   e.cacheControl,
	}
	if stale {
		update.Stale = proto.Bool(true)
	}
	if e.candidate != nil {
		update.Track = track.Enum()
		update.Rollout = proto.String(e.candidate.Version)
	}
	if sub.nodeHas[version] {
		update.UseCached = &chandlerv1.UseCached{}
	} else {
		update.Bundle = e.bundles[version]
	}
	sub.sent, sub.nonce, sub.answered = version, nonce, true
	sub.sess.out.Push(chandlerv1.ConfigResponse_builder{Update: update.Build()}.Build())
}

// serveAllLocked serves every subscription of a resource.
func (h *Hub) serveAllLocked(r *resource) {
	for sub := range r.subs {
		h.serveLocked(sub, false)
	}
	h.reportLocked(r.entry)
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
	sub.refused, sub.failing = "", ""
	sub.nodeHas[sub.acked] = true
	h.reportLocked(sub.res.entry)
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
		h.serveLocked(sub, true)
	case codes.ResourceExhausted, codes.Internal:
		// A node-side failure that may pass: try again later.
		delay := min(retryBase<<min(sub.attempts, 10), retryMax)
		sub.attempts++
		sub.failing = version
		log.Warn("driver could not write the bundle; will retry", "delay", delay.String())
		if sub.retry != nil {
			sub.retry.Stop()
		}
		sub.retry = time.AfterFunc(delay, func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if sess.subs[sub.id] != sub || sub.res.entry == nil {
				return
			}
			target, _ := sub.res.entry.target(sub.request.GetWorkload().GetPodUid())
			if target.Version == version && sub.acked != version {
				h.serveLocked(sub, true)
			}
		})
	default:
		// The bundle can never be written to that volume: not resent. The
		// provider hears of it, and may hold the rollout.
		sub.refused = version
		log.Error("driver refused the bundle")
	}
	h.reportLocked(sub.res.entry)
}

// reportLocked tells the provider how the entry's newest revision is faring
// on this replica's pods, if that changed since it was last told.
func (h *Hub) reportLocked(e *entry) {
	if e == nil {
		return
	}
	up := h.upstreams[e.key.provider]
	if up == nil || !up.connected() {
		return
	}
	newest := e.newest()
	now := feedback{revision: newest.Revision}
	for r := range e.refs {
		for sub := range r.subs {
			if target, _ := e.target(sub.request.GetWorkload().GetPodUid()); target.Version != newest.Version {
				continue
			}
			now.assigned++
			switch newest.Version {
			case sub.acked:
				now.written++
			case sub.refused:
				now.refused++
			case sub.failing:
				now.failing++
			}
		}
	}
	if now == e.reported && e.reportedOn == up.generation {
		return
	}
	// Tell the provider when a revision it heard about no longer has anyone.
	if e.reportedOn == up.generation && e.reported.revision != now.revision && e.reported.assigned > 0 {
		up.out.Push(chandlerv1.ProviderRequest_builder{
			Feedback: chandlerv1.Feedback_builder{
				CacheKey: proto.String(e.key.cacheKey),
				Revision: proto.String(e.reported.revision),
			}.Build(),
		}.Build())
	}
	e.reported, e.reportedOn = now, up.generation
	up.out.Push(chandlerv1.ProviderRequest_builder{
		Feedback: chandlerv1.Feedback_builder{
			CacheKey: proto.String(e.key.cacheKey),
			Revision: proto.String(now.revision),
			Assigned: proto.Uint32(now.assigned),
			Written:  proto.Uint32(now.written),
			Refused:  proto.Uint32(now.refused),
			Failing:  proto.Uint32(now.failing),
		}.Build(),
	}.Build())
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

// admit checks the quotas for a bundle about to become the current version of
// a key, which may be new.
func (h *Hub) admit(r *resource, key entryKey, size uint64) error {
	keys, held, variants := 0, uint64(0), map[string]bool{key.cacheKey: true}
	for k, e := range h.entries {
		if k.provider != key.provider {
			continue
		}
		keys++
		if k != key {
			held += e.current().Bytes
		}
	}
	for _, a := range h.aliases {
		if a.Provider == key.provider && a.URI == r.uri {
			variants[a.CacheKey] = true
		}
	}
	switch {
	case h.entries[key] == nil && keys >= h.quotas.MaxKeysPerProvider:
		return fmt.Errorf("provider %s already has %d cache keys, the limit", key.provider, h.quotas.MaxKeysPerProvider)
	case len(variants) > h.quotas.MaxVariantsPerURI:
		return fmt.Errorf("%s would resolve to more than %d cache keys", r.uri, h.quotas.MaxVariantsPerURI)
	case held+size > h.quotas.MaxBytesPerProvider:
		return fmt.Errorf("provider %s would hold more than %d bytes", key.provider, h.quotas.MaxBytesPerProvider)
	}
	return nil
}

// resourceID identifies a resource: the canonical URI and the vary attributes.
func resourceID(canonical string, attributes map[string]string) string {
	if len(attributes) == 0 {
		return canonical
	}
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(canonical)
	for _, name := range names {
		// NUL cannot appear in a URI, so the parts cannot run together.
		fmt.Fprintf(&b, "\x00%s=%s", name, attributes[name])
	}
	return b.String()
}
