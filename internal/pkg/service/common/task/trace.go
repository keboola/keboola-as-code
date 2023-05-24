package task

import (
	"context"
	"net"
	"sort"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type meters struct {
	taskDuration metric.Float64Histogram
}

func newMeters(meter metric.Meter) *meters {
	return &meters{
		taskDuration: histogram(meter, "keboola.go.task.duration", "Background task duration.", "ms"),
	}
}

func meterAttrs(task *Task, errType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("task_type", task.Type),
		attribute.Bool("is_success", task.IsSuccessful()),
		attribute.String("error_type", errType),
	}
}

func spanStartAttrs(task *Task) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("project_id", task.ProjectID.String()),
		attribute.String("task_id", task.TaskID.String()),
		attribute.String("task_type", task.Type),
		attribute.String("lock", task.Lock.Key()),
		attribute.String("node", task.Node),
		attribute.String("created_at", task.CreatedAt.String()),
	}
}

func spanEndAttrs(task *Task, errType string) []attribute.KeyValue {
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
			attribute.String("error", task.Error),
			attribute.String("error_type", errType),
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

func histogram(meter metric.Meter, name, desc, unit string) metric.Float64Histogram {
	return mustInstrument(meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit(unit)))
}

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}

func errorType(err error) string {
	var netErr net.Error
	errors.As(err, &netErr)
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case netErr != nil && netErr.Timeout():
		return "net_timeout"
	case netErr != nil:
		return "net"
	default:
		return "other"
	}
}
