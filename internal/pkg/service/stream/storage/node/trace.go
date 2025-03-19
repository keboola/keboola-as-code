package node

import (
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Metrics struct {
	Duration         metric.Float64Histogram
	Compressed       metric.Int64Counter
	Uncompressed     metric.Int64Counter
	FileImportFailed metric.Int64Counter
}

func NewMetrics(meter telemetry.Meter) *Metrics {
	return &Metrics{
		Duration: meter.FloatHistogram(
			"keboola.go.stream.operation.duration",
			"Duration of operator processing.",
			"ms",
			metric.WithExplicitBucketBoundaries(0, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000, 300000, 1000000),
		),
		Compressed:       meter.IntCounter("keboola.go.stream.operation.bytes.compressed", "Compressed bytes processed by operator.", "B"),
		Uncompressed:     meter.IntCounter("keboola.go.stream.operation.bytes.uncompressed", "Uncompressed bytes processed by operator.", "B"),
		FileImportFailed: meter.IntCounter("keboola.go.stream.operation.fileimport.failed", "Count of file imports that will be retried", "count"),
	}
}
