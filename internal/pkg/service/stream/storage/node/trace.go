package node

import (
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Metrics struct {
	Duration            metric.Float64Histogram
	Compressed          metric.Int64Counter
	Uncompressed        metric.Int64Counter
	FileRotationFailed  metric.Int64Histogram
	SliceRotationFailed metric.Int64Histogram
	FileImportFailed    metric.Int64Histogram
	SliceUploadFailed   metric.Int64Histogram
	FileCleanupFailed   metric.Int64Histogram
	JobCleanupFailed    metric.Int64Histogram
}

func NewMetrics(meter telemetry.Meter) *Metrics {
	return &Metrics{
		Duration: meter.FloatHistogram(
			"keboola.go.stream.operation.duration",
			"Duration of operator processing.",
			"ms",
			metric.WithExplicitBucketBoundaries(0, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000, 300000, 1000000),
		),
		Compressed:          meter.IntCounter("keboola.go.stream.operation.bytes.compressed", "Compressed bytes processed by operator.", "B"),
		Uncompressed:        meter.IntCounter("keboola.go.stream.operation.bytes.uncompressed", "Uncompressed bytes processed by operator.", "B"),
		FileRotationFailed:  meter.IntHistogram("keboola.go.stream.operation.filerotation.failed", "Count of failed file rotations", "count"),
		SliceRotationFailed: meter.IntHistogram("keboola.go.stream.operation.slicerotation.failed", "Count of failed slice rotations", "count"),
		FileImportFailed:    meter.IntHistogram("keboola.go.stream.operation.fileimport.failed", "Count of file imports that will be retried", "count"),
		SliceUploadFailed:   meter.IntHistogram("keboola.go.stream.operation.sliceupload.failed", "Count of slice uploads that will be retried", "count"),
		FileCleanupFailed:   meter.IntHistogram("keboola.go.stream.operation.filecleanup.failed", "Count of failed file cleanup tasks", "count"),
		JobCleanupFailed:    meter.IntHistogram("keboola.go.stream.operation.jobcleanup.failed", "Count of failed job cleanup tasks", "count"),
	}
}
