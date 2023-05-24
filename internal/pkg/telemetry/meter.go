package telemetry

import "go.opentelemetry.io/otel/metric"

func Histogram(meter metric.Meter, name, desc, unit string) metric.Float64Histogram {
	return mustInstrument(meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit(unit)))
}

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}
