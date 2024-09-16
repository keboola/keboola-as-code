package node

import (
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Metrics struct {
	Duration     metric.Float64Histogram
	Compressed   metric.Int64Counter
	Uncompressed metric.Int64Counter
}

func NewMetrics(meter telemetry.Meter) *Metrics {
	return &Metrics{
		Duration: meter.FloatHistogram(
			"keboola.go.stream.operation.duration",
			"Duration of operator processing.",
			"ms",
			metric.WithExplicitBucketBoundaries(0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000),
		),
		Compressed:   meter.IntCounter("keboola.go.stream.operation.bytes.compressed", "Compressed bytes processed by operator.", "B"),
		Uncompressed: meter.IntCounter("keboola.go.stream.operation.bytes.uncompressed", "Uncompressed bytes processed by operator.", "B"),
	}
}
