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
	acks, nacks, appReports, adopted, refusedBundles metric.Int64Counter
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
		metric.WithDescription("Bytes of the current version of every key held."), metric.WithUnit("By")))
	orphaned := telemetry.Must(m.Int64ObservableGauge("chandler.server.cache.orphaned_keys",
		metric.WithDescription("Keys whose resource was deleted at the origin and is served from the cache.")))
	rollouts := telemetry.Must(m.Int64ObservableGauge("chandler.server.rollouts",
		metric.WithDescription("Keys with a version released to a share of their subscribers only.")))

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
		for key, e := range h.entries {
			c := per[key.provider]
			if c == nil {
				continue
			}
			c.keys++
			c.bytes += int64(e.current().Bytes)
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
