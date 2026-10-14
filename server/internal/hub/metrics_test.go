package hub

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The numbers an operator would alert on are the ones the hub reports.
func TestMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	f := startRollout(t, Config{})
	f.provider.emitShare(f.watch, "n2", "r2", f.v2, half)
	for range f.in {
		f.node.send(t, ack(f.node.next(t).GetUpdate()))
	}

	read := func() map[string]int64 {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
		values := make(map[string]int64)
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				switch data := m.Data.(type) {
				case metricdata.Gauge[int64]:
					for _, p := range data.DataPoints {
						values[m.Name] += p.Value
					}
				case metricdata.Sum[int64]:
					for _, p := range data.DataPoints {
						values[m.Name] += p.Value
					}
				}
			}
		}
		return values
	}
	// Four pods, one resource, a rollout in progress, the provider connected;
	// four ACKs of v1 and two of v2.
	want := map[string]int64{
		"stevedore.server.subscriptions":      4,
		"stevedore.server.watches":            1,
		"stevedore.server.provider.connected": 1,
		"stevedore.server.cache.keys":         1,
		"stevedore.server.rollouts":           1,
		"stevedore.server.acks":               6,
	}
	waitFor(t, func() bool {
		got := read()
		for name, v := range want {
			if got[name] != v {
				return false
			}
		}
		return true
	})
	got := read()
	for name, want := range map[string]int64{
		"stevedore.server.cache.uri_variants":       1,
		"stevedore.server.cache.uri_variants.limit": 16,
		"stevedore.server.cache.keys.limit":         10000,
		"stevedore.server.namespace_uris":           1,
		"stevedore.server.namespace_uris.limit":     1000,
	} {
		if got[name] != want {
			t.Errorf("%s = %d, want %d", name, got[name], want)
		}
	}
	if got := read(); got["stevedore.server.cache.bytes"] <= 0 {
		t.Errorf("cache bytes = %d, want what the stable version weighs", got["stevedore.server.cache.bytes"])
	}
}
