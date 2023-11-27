package task_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestCleanup(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)
	tel := newTestTelemetryWithFilter(t)

	logs := ioutil.NewAtomicWriter()
	node, d := createNode(t, etcdCfg, logs, tel, "node1")
	logger := d.DebugLogger()
	logger.Truncate()
	tel.Reset()

	taskPrefix := etcdop.NewTypedPrefix[task.Task](task.DefaultTaskEtcdPrefix, d.EtcdSerde())

	// Add task without a finishedAt timestamp but too old - will be deleted
	createdAtRaw, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	createdAt := utctime.UTCTime(createdAtRaw)
	taskKey1 := task.Key{ProjectID: 123, TaskID: task.ID(fmt.Sprintf("%s/%s_%s", "some.task", createdAt.String(), "abcdef"))}
	task1 := task.Task{
		Key:        taskKey1,
		Type:       "some.task",
		CreatedAt:  createdAt,
		FinishedAt: nil,
		Node:       "node1",
		Lock:       "lock1",
		Result:     "",
		Error:      "err",
		Duration:   nil,
	}
	assert.NoError(t, taskPrefix.Key(taskKey1.String()).Put(client, task1).Do(ctx).Err())

	// Add task with a finishedAt timestamp in the past - will be deleted
	time2, _ := time.Parse(time.RFC3339, "2008-01-02T15:04:05+07:00")
	time2Key := utctime.UTCTime(time2)
	taskKey2 := task.Key{ProjectID: 456, TaskID: task.ID(fmt.Sprintf("%s/%s_%s", "other.task", createdAt.String(), "ghijkl"))}
	task2 := task.Task{
		Key:        taskKey2,
		Type:       "other.task",
		CreatedAt:  createdAt,
		FinishedAt: &time2Key,
		Node:       "node2",
		Lock:       "lock2",
		Result:     "res",
		Error:      "",
		Duration:   nil,
	}
	assert.NoError(t, taskPrefix.Key(taskKey2.String()).Put(client, task2).Do(ctx).Err())

	// Add task with a finishedAt timestamp before a moment - will be ignored
	time3 := time.Now()
	time3Key := utctime.UTCTime(time3)
	taskKey3 := task.Key{ProjectID: 789, TaskID: task.ID(fmt.Sprintf("%s/%s_%s", "third.task", createdAt.String(), "mnopqr"))}
	task3 := task.Task{
		Key:        taskKey3,
		Type:       "third.task",
		CreatedAt:  createdAt,
		FinishedAt: &time3Key,
		Node:       "node2",
		Lock:       "lock2",
		Result:     "res",
		Error:      "",
		Duration:   nil,
	}
	assert.NoError(t, taskPrefix.Key(taskKey3.String()).Put(client, task3).Do(ctx).Err())

	// Run the cleanup
	tel.Reset()
	assert.NoError(t, node.Cleanup())

	// Shutdown - wait for cleanup
	d.Process().Shutdown(errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
[node1][task][_system_/tasks.cleanup/%s]INFO  started task
[node1][task][_system_/tasks.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/tasks.cleanup"
[node1][task][_system_/tasks.cleanup/%s]DEBUG  deleted task "123/some.task/2006-01-02T08:04:05.000Z_abcdef"
[node1][task][_system_/tasks.cleanup/%s]DEBUG  deleted task "456/other.task/2006-01-02T08:04:05.000Z_ghijkl"
[node1][task][_system_/tasks.cleanup/%s]INFO  deleted "2" tasks
[node1][task][_system_/tasks.cleanup/%s]INFO  task succeeded (%s): deleted "2" tasks
[node1][task][_system_/tasks.cleanup/%s]DEBUG  lock released "runtime/lock/task/tasks.cleanup"
[node1]INFO  exiting (bye bye)
[node1][task]INFO  received shutdown request
[node1][task][etcd-session]INFO  closing etcd session
[node1][task][etcd-session]INFO  closed etcd session | %s
[node1][task]INFO  shutdown done
[node1][etcd-client]INFO  closing etcd connection
[node1][etcd-client]INFO  closed etcd connection | %s
[node1]INFO  exited
`, d.DebugLogger().AllMessages())

	// Check keys
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/789/third.task/2006-01-02T08:04:05.000Z_mnopqr
-----
{
  "projectId": 789,
  "taskId": "third.task/2006-01-02T08:04:05.000Z_mnopqr",
  "type": "third.task",
  "createdAt": "2006-01-02T08:04:05.000Z",
  "finishedAt": "%s",
  "node": "node2",
  "lock": "lock2",
  "result": "res"
}
>>>>>

<<<<<
task/_system_/tasks.cleanup/%s
-----
{
  "systemTask": true,
  "taskId": "tasks.cleanup/%s",
  "type": "tasks.cleanup",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/tasks.cleanup",
  "result": "deleted \"2\" tasks",
  "duration": %d
}
>>>>>
`)

	// Check spans
	tel.AssertSpans(t,
		tracetest.SpanStubs{
			{
				Name:     "keboola.go.task",
				SpanKind: trace.SpanKindInternal,
				SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    tel.TraceID(1),
					SpanID:     tel.SpanID(1),
					TraceFlags: trace.FlagsSampled,
				}),
				Status: tracesdk.Status{Code: codes.Ok},
				Attributes: []attribute.KeyValue{
					attribute.String("resource.name", "tasks.cleanup"),
					attribute.String("task_id", "<dynamic>"),
					attribute.String("task_type", "tasks.cleanup"),
					attribute.String("lock", "runtime/lock/task/tasks.cleanup"),
					attribute.String("node", "node1"),
					attribute.String("created_at", "<dynamic>"),
					attribute.Int64("task.cleanup.deletedTasksCount", 2),
					attribute.String("duration_sec", "<dynamic>"),
					attribute.String("finished_at", "<dynamic>"),
					attribute.Bool("is_success", true),
				},
			},
		},
		telemetry.WithSpanAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
			switch attr.Key {
			case "task_id", "created_at", "duration_sec", "finished_at":
				return attribute.String(string(attr.Key), "<dynamic>")
			}
			return attr
		}),
	)

	// Check metrics
	histBounds := []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000} // ms
	tel.AssertMetrics(t,
		[]metricdata.Metrics{
			{
				Name:        "keboola.go.task.running",
				Description: "Background running tasks count.",
				Data: metricdata.Sum[int64]{
					Temporality: 1,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 0,
							Attributes: attribute.NewSet(
								attribute.String("task_type", "tasks.cleanup"),
							),
						},
					},
				},
			},
			{
				Name:        "keboola.go.task.duration",
				Description: "Background task duration.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					Temporality: 1,
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						{
							Count:  1,
							Bounds: histBounds,
							Attributes: attribute.NewSet(
								attribute.String("task_type", "tasks.cleanup"),
								attribute.Bool("is_success", true),
							),
						},
					},
				},
			},
		},
	)
}
