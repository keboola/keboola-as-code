package telemetry

import (
	"go.opentelemetry.io/otel/metric"
)

type Meter interface {
	Counter(name, desc, unit string, opts ...metric.Int64CounterOption) metric.Int64Counter
	UpDownCounter(name, desc, unit string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter
	Histogram(name, desc, unit string, opts ...metric.Float64HistogramOption) metric.Float64Histogram
}

type meter struct {
	meter metric.Meter
}

func (m *meter) Counter(name, desc, unit string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return mustInstrument(m.meter.Int64Counter(name, opts...))
}

func (m *meter) UpDownCounter(name, desc, unit string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return mustInstrument(m.meter.Int64UpDownCounter(name, opts...))
}

func (m *meter) Histogram(name, desc, unit string, opts ...metric.Float64HistogramOption) metric.Float64Histogram {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return mustInstrument(m.meter.Float64Histogram(name, opts...))
}

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}
