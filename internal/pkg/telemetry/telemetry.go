package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	metricNoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const (
	// appName used by the app tracer and meter.
	appName = "github.com/keboola/keboola-as-code"
)

// Telemetry provides tracing and metrics collection implementations.
// Use Tracer() and Meter() to create an app-specify trace/meter.
// Use TracerProvider() and MeterProvider() to use a 3rd party instrumentations library.
type Telemetry interface {
	// Tracer for app-specific traces, it is used directly by the app code.
	// The app code uses the modified Tracer, with a modified End method,
	Tracer() Tracer
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
	tracer         Tracer
	meter          metric.Meter
}

func NewNopTelemetry() Telemetry {
	tel, _ := NewTelemetry(nil, nil)
	return tel
}

func NewTelemetry(tpFactory func() (trace.TracerProvider, error), mpFactory func() (metric.MeterProvider, error)) (Telemetry, error) {
	var err error
	var tracerProvider trace.TracerProvider
	var meterProvider metric.MeterProvider

	if tpFactory != nil {
		tracerProvider, err = tpFactory()
		if err != nil {
			return nil, err
		}
	}

	if mpFactory != nil {
		meterProvider, err = mpFactory()
		if err != nil {
			return nil, err
		}
	}

	if tracerProvider == nil {
		tracerProvider = trace.NewNoopTracerProvider()
	}
	if meterProvider == nil {
		meterProvider = metricNoop.NewMeterProvider()
	}
	return newTelemetry(tracerProvider, meterProvider), nil
}

func newTelemetry(tp trace.TracerProvider, mp metric.MeterProvider) *telemetry {
	return &telemetry{
		tracerProvider: tp,
		meterProvider:  mp,
		tracer:         &tracer{tp.Tracer(appName)},
		meter:          mp.Meter(appName),
	}
}

func (t *telemetry) Tracer() Tracer {
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
