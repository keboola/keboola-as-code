package task

import (
	"sort"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type meters struct {
	running  metric.Int64UpDownCounter
	duration metric.Float64Histogram
}

func newMeters(meter telemetry.Meter) *meters {
	return &meters{
		running:  meter.UpDownCounter("keboola.go.task.running", "Background running tasks count.", ""),
		duration: meter.Histogram("keboola.go.task.duration", "Background task duration.", "ms"),
	}
}

func meterStartAttrs(task *Task) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("task_type", task.Type),
	}
}

func meterEndAttrs(task *Task, r Result) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("task_type", task.Type),
		attribute.Bool("is_success", task.IsSuccessful()),
		attribute.Bool("is_application_error", r.IsApplicationError()),
		attribute.String("error_type", r.ErrorType()),
	}
}

func spanStartAttrs(task *Task) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("resource_name", task.Type),
		attribute.String("project_id", task.ProjectID.String()),
		attribute.String("task_id", task.TaskID.String()),
		attribute.String("task_type", task.Type),
		attribute.String("lock", task.Lock.Key()),
		attribute.String("node", task.Node),
		attribute.String("created_at", task.CreatedAt.String()),
	}
}

func spanEndAttrs(task *Task, r Result) []attribute.KeyValue {
	out := []attribute.KeyValue{
		attribute.Float64("duration_sec", task.Duration.Seconds()),
		attribute.String("finished_at", task.FinishedAt.String()),
		attribute.Bool("is_success", task.IsSuccessful()),
	}

	// Add result/error
	if task.IsSuccessful() {
		attribute.String("result", task.Result)
	} else {
		out = append(
			out,
			attribute.Bool("is_application_error", r.IsApplicationError()),
			attribute.String("error", task.Error),
			attribute.String("error_type", r.ErrorType()),
		)
	}

	// Add task outputs
	{
		var attrs []attribute.KeyValue
		for k, v := range task.Outputs {
			attrs = append(attrs, attribute.String("result_outputs."+k, cast.ToString(v)))
		}
		sort.SliceStable(attrs, func(i, j int) bool {
			return attrs[i].Key < attrs[j].Key
		})
		out = append(out, attrs...)
	}

	return out
}
