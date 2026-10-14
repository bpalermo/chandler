// Package telemetry sends a component's metrics to an OpenTelemetry collector
// over OTLP.
//
// Instrumentation uses the OpenTelemetry API, which does nothing until Setup
// installs an exporter. Where metrics go is configured the standard way, by
// environment: OTEL_EXPORTER_OTLP_ENDPOINT (or the _METRICS_ variant) names
// the collector, OTEL_METRIC_EXPORT_INTERVAL the period, and
// OTEL_RESOURCE_ATTRIBUTES adds attributes. With no endpoint set, nothing is
// exported and nothing is dialled.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// scope is the instrumentation scope of every Stevedore metric.
const scope = "github.com/bpalermo/stevedore"

// Meter returns the meter Stevedore's metrics are created on.
func Meter() metric.Meter { return otel.Meter(scope) }

// Enabled reports whether the environment names a collector.
func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}

// Setup starts exporting the process's metrics, as the given service and
// version, to the collector the environment names. It returns a function that
// sends what is pending and stops. With no collector configured it does
// nothing and returns a no-op.
func Setup(ctx context.Context, service, version string) (shutdown func(), err error) {
	if !Enabled() {
		return func() {}, nil
	}
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create the OTLP metric exporter: %w", err)
	}
	// The environment first, so that it can add attributes but not rename
	// the service by accident; then what this process knows about itself.
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(
			attribute.String("service.name", service),
			attribute.String("service.version", version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("describe this process: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	otel.SetMeterProvider(provider)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	}, nil
}

// Must panics when an instrument cannot be created, which only a programming
// error (an invalid name or unit) causes.
func Must[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}
