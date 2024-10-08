package telemetry

import (
	"go.opentelemetry.io/otel/metric"
)

// link:https://opentelemetry.io/docs/concepts/signals/metrics/#metric-instruments
type Meter interface {
	IntCounter(name, desc, unit string, opts ...metric.Int64CounterOption) metric.Int64Counter
	IntUpDownCounter(name, desc, unit string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter
	IntHistogram(name, desc, unit string, opts ...metric.Int64HistogramOption) metric.Int64Histogram
	FloatHistogram(name, desc, unit string, opts ...metric.Float64HistogramOption) metric.Float64Histogram
	IntObservableGauge(name, desc, unit string, callback metric.Int64Callback, opts ...metric.Int64ObservableGaugeOption) metric.Int64ObservableGauge
}

type meter struct {
	meter metric.Meter
}

func (m *meter) IntCounter(name, desc, unit string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return MustInstrument(m.meter.Int64Counter(name, opts...))
}

func (m *meter) IntUpDownCounter(name, desc, unit string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return MustInstrument(m.meter.Int64UpDownCounter(name, opts...))
}

func (m *meter) IntHistogram(name, desc, unit string, opts ...metric.Int64HistogramOption) metric.Int64Histogram {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return MustInstrument(m.meter.Int64Histogram(name, opts...))
}

func (m *meter) FloatHistogram(name, desc, unit string, opts ...metric.Float64HistogramOption) metric.Float64Histogram {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit))
	return MustInstrument(m.meter.Float64Histogram(name, opts...))
}

func (m *meter) IntObservableGauge(name, desc, unit string, callback metric.Int64Callback, opts ...metric.Int64ObservableGaugeOption) metric.Int64ObservableGauge {
	opts = append(opts, metric.WithDescription(desc), metric.WithUnit(unit), metric.WithInt64Callback(callback))
	return MustInstrument(m.meter.Int64ObservableGauge(name, opts...))
}

func MustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}
