package driver

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"

	"github.com/bpalermo/chandler/common/telemetry"
)

// metrics is what the driver measures. They cost nothing until the process
// sets up an exporter.
type metrics struct {
	publishes, applies metric.Float64Histogram
	fromCache          metric.Int64Counter
	appReports         metric.Int64Counter
}

func (d *Driver) instrument() {
	m := telemetry.Meter()
	d.metrics = metrics{
		publishes: telemetry.Must(m.Float64Histogram("chandler.csi.publish.duration",
			metric.WithDescription("Time a pod's volume took to mount, most of it waiting for its sources, by outcome."),
			metric.WithUnit("s"))),
		applies: telemetry.Must(m.Float64Histogram("chandler.csi.apply.duration",
			metric.WithDescription("Time to write one update into a volume, by outcome: OK, or the code it was NACKed with."),
			metric.WithUnit("s"))),
		fromCache: telemetry.Must(m.Int64Counter("chandler.csi.mounts_from_cache",
			metric.WithDescription("Volumes mounted from the node cache because the server could not supply a source."))),
		appReports: telemetry.Must(m.Int64Counter("chandler.csi.app_reports",
			metric.WithDescription("Verdicts applications reported on their volumes, by state."))),
	}
	volumes := telemetry.Must(m.Int64ObservableGauge("chandler.csi.volumes",
		metric.WithDescription("Pod volumes this driver serves.")))
	connected := telemetry.Must(m.Int64ObservableGauge("chandler.csi.server.connected",
		metric.WithDescription("1 while the driver has a stream to a config server, else 0.")))
	telemetry.Must(m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		d.mu.Lock()
		n := len(d.published)
		d.mu.Unlock()
		o.ObserveInt64(volumes, int64(n))
		up := int64(1)
		if d.server.Err() != nil {
			up = 0
		}
		o.ObserveInt64(connected, up)
		return nil
	}, volumes, connected))
}

// outcome names how an operation ended: "OK" or the gRPC code of its error.
func outcome(err error) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", status.Code(err).String()))
}

func since(start time.Time) float64 { return time.Since(start).Seconds() }
