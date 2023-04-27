package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// appName used by the app tracer and meter
	appName = "github.com/keboola/keboola-as-code"
)

// Telemetry provides tracing and metrics collection implementations.
// Use Tracer() and Meter() to create an app-specify trace/meter.
// Use TracerProvider() and MeterProvider() to use a 3rd party instrumentations library.
type Telemetry interface {
	// Tracer for app-specific traces, it is used directly by the app code.
	Tracer() trace.Tracer
	// TracerProvider for 3rd party instrumentations, it should not be used directly in the app code.
	TracerProvider() trace.TracerProvider
	// Meter for app-specific metrics, it is used directly by the app code.
	Meter() metric.Meter
	// MeterProvider for 3rd party instrumentations, it should not be used directly in the app code.
	MeterProvider() metric.MeterProvider
}

type telemetry struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	tracer         trace.Tracer
	meter          metric.Meter
}

func NewNopTelemetry() Telemetry {
	return NewTelemetry(nil, nil)
}

func NewTelemetry(tracerProvider trace.TracerProvider, meterProvider metric.MeterProvider) Telemetry {
	if tracerProvider == nil {
		tracerProvider = trace.NewNoopTracerProvider()
	}
	if meterProvider == nil {
		meterProvider = metric.NewNoopMeterProvider()
	}
	return &telemetry{
		tracerProvider: tracerProvider,
		meterProvider:  meterProvider,
		tracer:         tracerProvider.Tracer(appName),
		meter:          meterProvider.Meter(appName),
	}
}

func (t *telemetry) Tracer() trace.Tracer {
	return t.tracer
}

func (t *telemetry) TracerProvider() trace.TracerProvider {
	return t.tracerProvider
}

func (t *telemetry) Meter() metric.Meter {
	return t.meter
}

func (t *telemetry) MeterProvider() metric.MeterProvider {
	return t.meterProvider
}
