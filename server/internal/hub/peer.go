package hub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

// Peers configures how a replica finds the others.
type Peers struct {
	// Service is the DNS name and port of the headless Service that lists
	// the ready replicas, "host:port". Empty disables peer sync.
	Service string
	// Self is this replica's own address as that Service lists it, so it
	// does not sync with itself.
	Self string
	// Interval is how often one peer is synced with.
	Interval time.Duration
	// WarmUp bounds the initial sync, after which the replica reports ready
	// whatever it managed to copy.
	WarmUp time.Duration
	// Resolve looks up the peers; nil means DNS.
	Resolve func(ctx context.Context) ([]string, error)
}

// ListKeys implements chandlerv1.PeerSyncServer: everything this replica
// holds, one entry per key.
func (h *Hub) ListKeys(_ *chandlerv1.ListKeysRequest, stream chandlerv1.PeerSync_ListKeysServer) error {
	h.mu.Lock()
	aliases := make(map[entryKey][]*chandlerv1.Alias)
	for _, a := range h.aliases {
		key := entryKey{a.Provider, a.CacheKey}
		aliases[key] = append(aliases[key], chandlerv1.Alias_builder{Uri: proto.String(a.URI), Attributes: a.Attributes}.Build())
	}
	now := h.now()
	entries := make([]*chandlerv1.KeyEntry, 0, len(h.entries))
	for key, e := range h.entries {
		// What is live or referenced right now is so as of now.
		refreshed, referenced := e.lastRefreshed, e.lastReferenced
		if len(e.refs) > 0 {
			referenced = now
			if e.live() {
				refreshed = now
			}
		}
		ke := chandlerv1.KeyEntry_builder{
			Provider:       proto.String(key.provider),
			CacheKey:       proto.String(key.cacheKey),
			CurrentVersion: proto.String(e.current().Version),
			Aliases:        aliases[key],
			CacheControl:   e.cacheControl,
			LastRefreshed:  timestamppb.New(refreshed),
			LastReferenced: timestamppb.New(referenced),
			Orphaned:       proto.Bool(e.orphaned),
		}
		for _, v := range e.versions {
			ke.Versions = append(ke.Versions, chandlerv1.KeyVersion_builder{
				Version: proto.String(v.Version), Revision: proto.String(v.Revision),
			}.Build())
		}
		entries = append(entries, ke.Build())
	}
	h.mu.Unlock()

	for _, ke := range entries {
		if err := stream.Send(ke); err != nil {
			return err
		}
	}
	return nil
}

// GetBundle implements chandlerv1.PeerSyncServer.
func (h *Hub) GetBundle(_ context.Context, req *chandlerv1.GetBundleRequest) (*chandlerv1.GetBundleResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.entries {
		if b, ok := e.bundles[req.GetVersion()]; ok {
			return chandlerv1.GetBundleResponse_builder{Bundle: b}.Build(), nil
		}
	}
	return nil, grpcstatus.Errorf(codes.NotFound, "version %s is not held", req.GetVersion())
}

// RunPeers keeps this replica's cache in step with the others until ctx ends.
// It first copies what the ready peers hold, then calls ready, then syncs
// with one peer every interval.
//
// ready is called exactly once: after the first sync, when there is no peer
// to sync with (the first replica, or the whole fleet was lost), or when the
// warm-up time runs out. Until then the replica must not report ready, so
// that a rolling restart does not proceed past a replica that holds nothing.
func (h *Hub) RunPeers(ctx context.Context, peers Peers, ready func()) {
	if peers.Service == "" {
		ready()
		<-ctx.Done()
		return
	}
	resolve := peers.Resolve
	if resolve == nil {
		resolve = func(ctx context.Context) ([]string, error) { return resolveService(ctx, peers.Service, peers.Self) }
	}

	warm, cancel := context.WithTimeout(ctx, peers.WarmUp)
	addresses, err := resolve(warm)
	switch {
	case err != nil || len(addresses) == 0:
		h.log.Info("no ready peer to warm up from; starting with what the cache directory holds", "error", err)
	default:
		// Every ready peer, not just one: a key may so far be held by only
		// the replica whose drivers subscribe to it.
		synced := 0
		for _, address := range addresses {
			if err := h.syncWith(warm, address); err != nil {
				h.log.Warn("warm-up from a peer failed", "peer", address, "error", err)
				continue
			}
			synced++
		}
		h.log.Info("warm-up finished", "peers", synced, "entries", h.entryCount())
	}
	cancel()
	ready()

	ticker := time.NewTicker(peers.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		addresses, err := resolve(ctx)
		if err != nil || len(addresses) == 0 {
			continue
		}
		address := addresses[rand.IntN(len(addresses))]
		step, cancel := context.WithTimeout(ctx, peers.Interval)
		if err := h.syncWith(step, address); err != nil && ctx.Err() == nil {
			h.log.Warn("sync with a peer failed", "peer", address, "error", err)
		}
		cancel()
	}
}

func (h *Hub) entryCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.entries)
}

// resolveService returns the addresses a headless Service lists, without this
// replica's own.
func resolveService(ctx context.Context, service, self string) ([]string, error) {
	host, port, err := net.SplitHostPort(service)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	var addresses []string
	for _, ip := range ips {
		if address := net.JoinHostPort(ip, port); address != self {
			addresses = append(addresses, address)
		}
	}
	return addresses, nil
}

// syncWith copies from one peer what this replica lacks.
func (h *Hub) syncWith(ctx context.Context, address string) error {
	// Plaintext: transport security between components is not implemented yet.
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := chandlerv1.NewPeerSyncClient(conn)

	stream, err := client.ListKeys(ctx, chandlerv1.ListKeysRequest_builder{ServerId: proto.String(h.id)}.Build())
	if err != nil {
		return err
	}
	for {
		ke, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := h.adopt(ctx, client, ke); err != nil {
			h.log.Warn("could not adopt a peer's entry", "peer", address,
				"provider", ke.GetProvider(), "cache_key", ke.GetCacheKey(), "error", err)
		}
	}
}

// adopt merges one entry of a peer into the cache.
//
// A replica with a live provider watch for the key trusts its own stream.
// Otherwise the entry most recently confirmed with the provider wins. The
// last-referenced time is merged by taking the later, so that every replica
// drops an unused key at the same moment and none brings it back.
func (h *Hub) adopt(ctx context.Context, client chandlerv1.PeerSyncClient, ke *chandlerv1.KeyEntry) error {
	key := entryKey{ke.GetProvider(), ke.GetCacheKey()}
	refreshed, referenced := ke.GetLastRefreshed().AsTime(), ke.GetLastReferenced().AsTime()
	if len(ke.GetVersions()) == 0 {
		return nil
	}

	h.mu.Lock()
	up := h.upstreams[key.provider]
	e := h.entries[key]
	now := h.now()
	wanted := false
	switch {
	case up == nil || up.target == "":
		// A provider this replica is not configured for.
	case e == nil:
		// Not something every replica already let go of.
		wanted = now.Sub(referenced) <= h.grace && h.providerKeysLocked(key.provider) < h.quotas.MaxKeysPerProvider
	default:
		if referenced.After(e.lastReferenced) {
			e.lastReferenced = referenced
		}
		wanted = !e.live() && refreshed.After(e.lastRefreshed)
	}
	var missing []string
	if wanted {
		for _, v := range ke.GetVersions() {
			if e == nil || e.bundles[v.GetVersion()] == nil {
				missing = append(missing, v.GetVersion())
			}
		}
	}
	h.mu.Unlock()
	if !wanted {
		return nil
	}

	// Fetch outside the lock; verify before trusting: a peer cannot hand
	// over altered bytes.
	fetched := make(map[string]*chandlerv1.Bundle, len(missing))
	for _, version := range missing {
		resp, err := client.GetBundle(ctx, chandlerv1.GetBundleRequest_builder{Version: proto.String(version)}.Build())
		if err != nil {
			if version == ke.GetVersions()[0].GetVersion() {
				return fmt.Errorf("fetch %s: %w", version, err)
			}
			continue // an older version the peer has since dropped
		}
		b := resp.GetBundle()
		if !bundle.Verify(b, version) {
			return fmt.Errorf("the peer's bundle does not match its version %s", version)
		}
		if err := bundle.Validate(b, h.limits); err != nil {
			return fmt.Errorf("the peer's bundle %s cannot be delivered: %w", version, err)
		}
		if h.store != nil && !ke.GetCacheControl().GetNoStore() {
			if err := h.store.Put(version, b); err != nil {
				return err
			}
		}
		fetched[version] = b
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	e = h.entries[key]
	if e != nil && (e.live() || !refreshed.After(e.lastRefreshed)) {
		return nil // the provider, or a fresher peer, got here first
	}
	if e == nil {
		e = &entry{key: key, bundles: make(map[string]*chandlerv1.Bundle), refs: make(map[*resource]struct{})}
	}
	var versions []held
	bundles := make(map[string]*chandlerv1.Bundle)
	for _, v := range ke.GetVersions() {
		b := fetched[v.GetVersion()]
		if b == nil {
			b = e.bundles[v.GetVersion()]
		}
		if b == nil || len(versions) == VersionsPerKey {
			continue
		}
		versions = append(versions, held{Version: v.GetVersion(), Revision: v.GetRevision(), Bytes: bundle.Size(b)})
		bundles[v.GetVersion()] = b
	}
	if len(versions) == 0 || versions[0].Version != ke.GetVersions()[0].GetVersion() {
		return nil
	}
	e.versions, e.bundles = versions, bundles
	e.cacheControl, e.lastRefreshed, e.orphaned = ke.GetCacheControl(), refreshed, ke.GetOrphaned()
	if referenced.After(e.lastReferenced) {
		e.lastReferenced = referenced
	}
	h.entries[key] = e
	for _, a := range ke.GetAliases() {
		id := resourceID(a.GetUri(), a.GetAttributes())
		h.aliases[id] = alias{Provider: key.provider, CacheKey: key.cacheKey, URI: a.GetUri(), Attributes: a.GetAttributes()}
		// A resource that was waiting with nothing to serve now has an entry.
		if r := h.resources[id]; r != nil && r.entry == nil {
			h.bindLocked(r, e)
		}
	}
	h.saveLocked()
	// Pods subscribed here get the version another replica learned of.
	for r := range e.refs {
		h.serveAllLocked(r)
	}
	return nil
}

// providerKeysLocked counts the cache keys held for one provider.
func (h *Hub) providerKeysLocked(provider string) int {
	n := 0
	for key := range h.entries {
		if key.provider == provider {
			n++
		}
	}
	return n
}
