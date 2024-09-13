package task

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type metrics struct {
	running  metric.Int64UpDownCounter
	duration metric.Float64Histogram
}

func newMetrics(meter telemetry.Meter) *metrics {
	return &metrics{
		running:  meter.IntUpDownCounter("keboola.go.task.running", "Background running tasks count.", ""),
		duration: meter.FloatHistogram("keboola.go.task.duration", "Background task duration.", "ms"),
	}
}

func meterStartAttrs(task *Task) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("task_type", task.Type),
	}
}

func meterEndAttrs(task *Task, r Result) []attribute.KeyValue {
	out := []attribute.KeyValue{
		attribute.String("task_type", task.Type),
		attribute.Bool("is_success", task.IsSuccessful()),
	}
	if r.IsError() {
		out = append(out,
			attribute.Bool("is_application_error", !isUserError(r.Error)),
			attribute.String("error_type", telemetry.ErrorType(r.Error)),
		)
	}
	return out
}

func spanStartAttrs(task *Task) []attribute.KeyValue {
	out := []attribute.KeyValue{
		attribute.String("resource.name", task.Type),
		attribute.String("task_id", task.TaskID.String()),
		attribute.String("task_type", task.Type),
		attribute.String("lock", task.Lock.Key()),
		attribute.String("node", task.Node),
		attribute.String("created_at", task.CreatedAt.String()),
	}

	if !task.IsSystemTask() {
		out = append(out, attribute.String("project_id", task.ProjectID.String()))
	}

	return out
}

func spanEndAttrs(task *Task, r Result) []attribute.KeyValue {
	out := []attribute.KeyValue{
		attribute.Float64("duration_sec", task.Duration.Seconds()),
		attribute.String("finished_at", task.FinishedAt.String()),
		attribute.Bool("is_success", task.IsSuccessful()),
	}

	// Add result/error
	if task.IsSuccessful() {
		out = append(
			out,
			attribute.String("result", task.Result),
		)
	} else {
		out = append(
			out,
			attribute.Bool("is_application_error", !isUserError(r.Error)),
			attribute.String("error", task.Error),
			attribute.String("error_type", telemetry.ErrorType(r.Error)),
		)
	}

	return out
}
