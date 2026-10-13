package hub

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/bpalermo/chandler/common/telemetry"
)

// metrics is what the hub counts as it goes. What can be read off its state
// (subscriptions, cache contents, rollouts) is observed when exported instead.
type metrics struct {
	acks, nacks, appReports, adopted, refusedBundles, resent, evictions metric.Int64Counter
}

func providerAttr(name string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("provider", name))
}

// instrument creates the hub's metrics. They cost nothing until a process
// sets up an exporter.
func (h *Hub) instrument() {
	m := telemetry.Meter()
	h.metrics = metrics{
		acks: telemetry.Must(m.Int64Counter("chandler.server.acks",
			metric.WithDescription("Updates drivers reported written."))),
		nacks: telemetry.Must(m.Int64Counter("chandler.server.nacks",
			metric.WithDescription("Updates drivers could not write, by gRPC code."))),
		appReports: telemetry.Must(m.Int64Counter("chandler.server.app_reports",
			metric.WithDescription("Verdicts of applications on versions they were given, by state."))),
		adopted: telemetry.Must(m.Int64Counter("chandler.server.peer.adopted",
			metric.WithDescription("Cache entries copied from another replica."))),
		resent: telemetry.Must(m.Int64Counter("chandler.server.resent",
			metric.WithDescription("Updates sent a second time because the driver did not answer the first."))),
		evictions: telemetry.Must(m.Int64Counter("chandler.server.cache.evictions",
			metric.WithDescription("Cache entries nothing used that were dropped, by reason: unused for the grace period, or to stay within a quota."))),
		refusedBundles: telemetry.Must(m.Int64Counter("chandler.server.provider.refused",
			metric.WithDescription("Bundles refused to a provider, by gRPC code."))),
	}

	subscriptions := telemetry.Must(m.Int64ObservableGauge("chandler.server.subscriptions",
		metric.WithDescription("Subscriptions of pod volumes served by this replica.")))
	watches := telemetry.Must(m.Int64ObservableGauge("chandler.server.watches",
		metric.WithDescription("Resources this replica watches at a provider.")))
	connected := telemetry.Must(m.Int64ObservableGauge("chandler.server.provider.connected",
		metric.WithDescription("1 while this replica has a stream to the provider, else 0.")))
	keys := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.keys",
		metric.WithDescription("Cache keys held, against the per-provider quota.")))
	bytes := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.bytes",
		metric.WithDescription("Bytes of content held, against the per-provider quota: every version of every key, shared content counted once."), metric.WithUnit("By")))
	orphaned := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.orphaned_keys",
		metric.WithDescription("Keys whose resource was deleted at the origin and is served from the cache.")))
	rollouts := telemetry.Must(m.Int64ObservableGauge("chandler.server.rollouts",
		metric.WithDescription("Keys with a version released to a share of their subscribers only.")))

	keysLimit := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.keys.limit",
		metric.WithDescription("The most cache keys one provider may issue: the quota chandler.server.cache.keys is held against.")))
	bytesLimit := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.bytes.limit",
		metric.WithDescription("The most content held for one provider: the quota chandler.server.cache.bytes is held against."), metric.WithUnit("By")))
	variants := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.uri_variants",
		metric.WithDescription("The most cache keys any one URI of the provider resolves to; a provider varying on too much shows here before it hits the limit.")))
	variantsLimit := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.uri_variants.limit",
		metric.WithDescription("The most cache keys one URI may resolve to.")))
	namespaceURIs := telemetry.Must(m.Int64ObservableGauge("chandler.server.namespace_uris",
		metric.WithDescription("The most distinct URIs the pods of any one namespace are subscribed to.")))
	namespaceURIsLimit := telemetry.Must(m.Int64ObservableGauge("chandler.server.namespace_uris.limit",
		metric.WithDescription("The most distinct URIs the pods of one namespace may be subscribed to.")))
	syncAge := telemetry.Must(m.Float64ObservableGauge("chandler.server.peer.sync_age",
		metric.WithDescription("Time since this replica last synced its cache with a peer; not reported before the first sync."),
		metric.WithUnit("s")))
	telemetry.Must(m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		h.mu.Lock()
		perURI := make(map[string]map[string]map[string]bool) // provider -> URI -> keys
		for _, a := range h.aliases {
			if perURI[a.Provider] == nil {
				perURI[a.Provider] = make(map[string]map[string]bool)
			}
			if perURI[a.Provider][a.URI] == nil {
				perURI[a.Provider][a.URI] = make(map[string]bool)
			}
			perURI[a.Provider][a.URI][a.CacheKey] = true
		}
		widest := make(map[string]int64)
		for name := range h.upstreams {
			widest[name] = 0
		}
		for provider, uris := range perURI {
			for _, keys := range uris {
				widest[provider] = max(widest[provider], int64(len(keys)))
			}
		}
		busiest := int64(0)
		for _, uris := range h.namespaces {
			busiest = max(busiest, int64(len(uris)))
		}
		synced := h.lastSync
		now := h.now()
		h.mu.Unlock()

		for name, n := range widest {
			attr := providerAttr(name)
			o.ObserveInt64(variants, n, attr)
			o.ObserveInt64(variantsLimit, int64(h.quotas.MaxVariantsPerURI), attr)
			maxKeys, maxBytes := h.quota(name)
			o.ObserveInt64(keysLimit, int64(maxKeys), attr)
			o.ObserveInt64(bytesLimit, int64(maxBytes), attr)
		}
		o.ObserveInt64(namespaceURIs, busiest)
		o.ObserveInt64(namespaceURIsLimit, int64(h.quotas.MaxURIsPerNamespace))
		if !synced.IsZero() {
			o.ObserveFloat64(syncAge, now.Sub(synced).Seconds())
		}
		return nil
	}, keysLimit, bytesLimit, variants, variantsLimit, namespaceURIs, namespaceURIsLimit, syncAge))

	telemetry.Must(m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		type counts struct{ subscriptions, watches, connected, keys, bytes, orphaned, rollouts int64 }
		h.mu.Lock()
		per := make(map[string]*counts)
		for name, up := range h.upstreams {
			c := &counts{}
			if up.connected() {
				c.connected = 1
			}
			per[name] = c
		}
		for _, r := range h.resources {
			c := per[r.up.name]
			if c == nil {
				continue
			}
			c.subscriptions += int64(len(r.subs))
			if r.watchID != "" {
				c.watches++
			}
		}
		for name, c := range per {
			c.bytes = int64(h.usageLocked(name).bytes)
		}
		for key, e := range h.entries {
			c := per[key.provider]
			if c == nil {
				continue
			}
			c.keys++
			if e.orphaned {
				c.orphaned++
			}
			if e.candidate != nil {
				c.rollouts++
			}
		}
		h.mu.Unlock()
		for name, c := range per {
			attr := providerAttr(name)
			o.ObserveInt64(subscriptions, c.subscriptions, attr)
			o.ObserveInt64(watches, c.watches, attr)
			o.ObserveInt64(connected, c.connected, attr)
			o.ObserveInt64(keys, c.keys, attr)
			o.ObserveInt64(bytes, c.bytes, attr)
			o.ObserveInt64(orphaned, c.orphaned, attr)
			o.ObserveInt64(rollouts, c.rollouts, attr)
		}
		return nil
	}, subscriptions, watches, connected, keys, bytes, orphaned, rollouts))
}
