package task_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestSuccessfulTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(runtime/distribution/)")

	lock := "my-lock"
	taskType := "some.task"
	tKey := task.Key{
		ProjectID: 123,
		TaskID:    task.ID("my-receiver/my-export/" + taskType),
	}

	logs := ioutil.NewAtomicWriter()
	tel := newTestTelemetryWithFilter(t)

	// Create nodes
	node1, _ := createNode(t, ctx, etcdCfg, logs, tel, "node1")
	node2, _ := createNode(t, ctx, etcdCfg, logs, tel, "node2")
	logs.Truncate()
	tel.Reset()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	_, err := node1.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			defer close(taskDone)
			<-taskWork

			logger.Info(ctx, "some message from the task (1)")
			return task.OkResult("some result (1)").
				WithOutput("key", "value").
				WithOutputsFrom(map[string]int{"int1": 1, "int2": 2})
		},
	})
	assert.NoError(t, err)
	_, err = node2.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			assert.Fail(t, "should not be called")
			return task.ErrResult(errors.New("the task should not be called"))
		},
	})
	assert.NoError(t, err)

	// Check etcd state during task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/lock/task/my-lock (lease)
-----
node1
>>>>>

<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock"
}
>>>>>
`, ignoredEtcdKeys)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result (1)",
  "outputs": {
    "int1": 1,
    "int2": 2,
    "key": "value"
  },
  "duration": %d
}
>>>>>
`, ignoredEtcdKeys)

	// Start another task with the same lock (lock is free)
	taskWork = make(chan struct{})
	taskDone = make(chan struct{})
	taskEntity, err := node2.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			defer close(taskDone)
			<-taskWork
			logger.Info(ctx, "some message from the task (2)")
			return task.OkResult("some result (2)")
		},
	})
	assert.NoError(t, err)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Get task
	{
		result, err := node1.GetTask(taskEntity.Key).Do(ctx).ResultOrErr()
		if assert.NoError(t, err) {
			assert.Equal(t, keboola.ProjectID(123), result.ProjectID)
			assert.True(t, strings.HasPrefix(result.TaskID.String(), "my-receiver/my-export/some.task/"), result.TaskID.String())
			assert.Equal(t, "some.task", result.Type)
		}
	}

	// Check etcd state after second task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result (1)",
  "outputs": {
    "int1": 1,
    "int2": 2,
    "key": "value"
  },
  "duration": %d
}
>>>>>

<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node2",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result (2)",
  "duration": %d
}
>>>>>
`, ignoredEtcdKeys)

	// Check logs
	log.AssertJSONMessages(t, `
{"level":"info","message":"started task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"task ignored, the lock \"runtime/lock/task/my-lock\" is in use","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"info","message":"some message from the task (1)","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"task succeeded (%s): some result (1)","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock released \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"started task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"info","message":"some message from the task (2)","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"info","message":"task succeeded (%s): some result (2)","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"debug","message":"lock released \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
`, logs.String())

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
					attribute.String("resource.name", "some.task"),
					attribute.String("task_id", "<dynamic>"),
					attribute.String("task_type", "some.task"),
					attribute.String("lock", "<dynamic>"),
					attribute.String("node", "node1"),
					attribute.String("created_at", "<dynamic>"),
					attribute.String("project_id", "123"),
					attribute.String("duration_sec", "<dynamic>"),
					attribute.String("finished_at", "<dynamic>"),
					attribute.Bool("is_success", true),
					attribute.String("result", "some result (1)"),
					attribute.String("result_outputs.int1", "1"),
					attribute.String("result_outputs.int2", "2"),
					attribute.String("result_outputs.key", "value"),
				},
			},
			{
				Name:     "keboola.go.task",
				SpanKind: trace.SpanKindInternal,
				SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    tel.TraceID(2),
					SpanID:     tel.SpanID(2),
					TraceFlags: trace.FlagsSampled,
				}),
				Status: tracesdk.Status{Code: codes.Ok},
				Attributes: []attribute.KeyValue{
					attribute.String("resource.name", "some.task"),
					attribute.String("task_id", "<dynamic>"),
					attribute.String("task_type", "some.task"),
					attribute.String("lock", "<dynamic>"),
					attribute.String("node", "node2"),
					attribute.String("created_at", "<dynamic>"),
					attribute.String("project_id", "123"),
					attribute.String("duration_sec", "<dynamic>"),
					attribute.String("finished_at", "<dynamic>"),
					attribute.Bool("is_success", true),
					attribute.String("result", "some result (2)"),
				},
			},
		},
		telemetry.WithSpanAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
			switch attr.Key {
			case "task_id", "lock", "created_at", "duration_sec", "finished_at":
				return attribute.String(string(attr.Key), "<dynamic>")
			}
			return attr
		}),
	)

	// Check metrics
	histBounds := []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000} // ms
	tel.AssertMetrics(t, []metricdata.Metrics{
		{
			Name:        "keboola.go.task.running",
			Description: "Background running tasks count.",
			Data: metricdata.Sum[int64]{
				Temporality: 1,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 0,
						Attributes: attribute.NewSet(
							attribute.String("task_type", "some.task"),
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
						Count:  2,
						Bounds: histBounds,
						Attributes: attribute.NewSet(
							attribute.String("task_type", "some.task"),
							attribute.Bool("is_success", true),
						),
					},
				},
			},
		},
	})
}

func TestFailedTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(runtime/distribution/)")

	lock := "my-lock"
	taskType := "some.task"
	tKey := task.Key{
		ProjectID: 123,
		TaskID:    task.ID("my-receiver/my-export/" + taskType),
	}

	logs := ioutil.NewAtomicWriter()
	tel := newTestTelemetryWithFilter(t)

	// Create nodes
	node1, _ := createNode(t, ctx, etcdCfg, logs, tel, "node1")
	node2, _ := createNode(t, ctx, etcdCfg, logs, tel, "node2")
	logs.Truncate()
	tel.Reset()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	_, err := node1.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			defer close(taskDone)
			<-taskWork
			logger.Info(ctx, "some message from the task (1)")
			return task.
				ErrResult(task.WrapUserError(errors.New("some error (1) - expected"))).
				WithOutput("key", "value")
		},
	})
	assert.NoError(t, err)
	_, err = node2.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			assert.Fail(t, "should not be called")
			return task.ErrResult(errors.New("the task should not be called"))
		},
	})
	assert.NoError(t, err)

	// Check etcd state during task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/lock/task/my-lock (lease)
-----
node1
>>>>>

<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock"
}
>>>>>
`, ignoredEtcdKeys)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock",
  "error": "some error (1) - expected",
  "userError": {
    "name": "unknownError",
    "message": "Unknown error",
    "exceptionId": "test-service-%s"
  },
  "outputs": {
    "key": "value"
  },
  "duration": %d
}
>>>>>
`, ignoredEtcdKeys)

	// Start another task with the same lock (lock is free)
	taskWork = make(chan struct{})
	taskDone = make(chan struct{})
	_, err = node2.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			defer close(taskDone)
			<-taskWork
			logger.Info(ctx, "some message from the task (2)")

			return task.ErrResult(svcerrors.NewInsufficientStorageError(false, errors.New("no space right on device")))
		},
	})
	assert.NoError(t, err)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after second task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock",
  "error": "some error (1) - expected",
  "userError": {
    "name": "unknownError",
    "message": "Unknown error",
    "exceptionId": "test-service-%s"
  },
  "outputs": {
    "key": "value"
  },
  "duration": %d
}
>>>>>

<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node2",
  "lock": "runtime/lock/task/my-lock",
  "error": "no space right on device",
  "userError": {
    "name": "insufficientStorage",
    "message": "No space right on device.",
    "exceptionId": "test-service-%s"
  },
  "duration": %d
}
>>>>>
`, ignoredEtcdKeys)

	// Check logs
	log.AssertJSONMessages(t, `
{"level":"info","message":"started task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"task ignored, the lock \"runtime/lock/task/my-lock\" is in use","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"info","message":"some message from the task (1)","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"warn","message":"task failed (%s): some error (1) - expected [%s] (*task.UserError):\n- some error (1) - expected [%s]","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock released \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"started task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"info","message":"some message from the task (2)","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"warn","message":"task failed (%s): no space right on device","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
{"level":"debug","message":"lock released \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node2"}
`, logs.String())

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
				Status: tracesdk.Status{Code: codes.Error, Description: "some error (1) - expected"},
				Attributes: []attribute.KeyValue{
					attribute.String("resource.name", "some.task"),
					attribute.String("task_id", "<dynamic>"),
					attribute.String("task_type", "some.task"),
					attribute.String("lock", "<dynamic>"),
					attribute.String("node", "node1"),
					attribute.String("created_at", "<dynamic>"),
					attribute.String("project_id", "123"),
					attribute.String("duration_sec", "<dynamic>"),
					attribute.String("finished_at", "<dynamic>"),
					attribute.Bool("is_success", false),
					attribute.Bool("is_application_error", false),
					attribute.String("error", "some error (1) - expected"),
					attribute.String("error_type", "other"),
					attribute.String("result_outputs.key", "value"),
				},
				Events: []tracesdk.Event{
					{
						Name: "exception",
						Attributes: []attribute.KeyValue{
							attribute.String("exception.type", "*task.UserError"),
							attribute.String("exception.message", "some error (1) - expected"),
						},
					},
				},
			},
			{
				Name:     "keboola.go.task",
				SpanKind: trace.SpanKindInternal,
				SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    tel.TraceID(2),
					SpanID:     tel.SpanID(2),
					TraceFlags: trace.FlagsSampled,
				}),
				Status: tracesdk.Status{Code: codes.Error, Description: "no space right on device"},
				Attributes: []attribute.KeyValue{
					attribute.String("resource.name", "some.task"),
					attribute.String("task_id", "<dynamic>"),
					attribute.String("task_type", "some.task"),
					attribute.String("lock", "<dynamic>"),
					attribute.String("node", "node2"),
					attribute.String("created_at", "<dynamic>"),
					attribute.String("project_id", "123"),
					attribute.String("duration_sec", "<dynamic>"),
					attribute.String("finished_at", "<dynamic>"),
					attribute.Bool("is_success", false),
					attribute.Bool("is_application_error", true),
					attribute.String("error", "no space right on device"),
					attribute.String("error_type", "insufficientStorage"),
				},
				Events: []tracesdk.Event{
					{
						Name: "exception",
						Attributes: []attribute.KeyValue{
							attribute.String("exception.type", "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors.InsufficientStorageError"),
							attribute.String("exception.message", "no space right on device"),
						},
					},
				},
			},
		},
		telemetry.WithSpanAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
			switch attr.Key {
			case "task_id", "lock", "created_at", "duration_sec", "finished_at":
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
								attribute.String("task_type", "some.task"),
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
						// Expected error, so it will not be taken as an error in the metrics.
						{
							Count:  1,
							Bounds: histBounds,
							Attributes: attribute.NewSet(
								attribute.String("task_type", "some.task"),
								attribute.Bool("is_success", false),
								attribute.Bool("is_application_error", false),
								attribute.String("error_type", "other"),
							),
						},
						// Unexpected error
						{
							Count:  1,
							Bounds: histBounds,
							Attributes: attribute.NewSet(
								attribute.String("task_type", "some.task"),
								attribute.Bool("is_success", false),
								attribute.Bool("is_application_error", true),
								attribute.String("error_type", "insufficientStorage"),
							),
						},
					},
				},
			},
		},
		telemetry.WithDataPointSortKey(func(attrs attribute.Set) string {
			if v, _ := attrs.Value("is_application_error"); v.AsBool() {
				return "1"
			}
			return "0"
		}),
	)
}

func TestTaskTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(runtime/distribution/)")

	lock := "my-lock"
	taskType := "some.task"
	tKey := task.Key{
		ProjectID: 123,
		TaskID:    task.ID("my-receiver/my-export/" + taskType),
	}

	tel := newTestTelemetryWithFilter(t)

	// Create node and
	node1, d := createNode(t, ctx, etcdCfg, nil, tel, "node1")
	logger := d.DebugLogger()
	logger.Truncate()
	tel.Reset()

	// Start task
	_, err := node1.StartTask(ctx, task.Config{
		Key:  tKey,
		Type: taskType,
		Lock: lock,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, 10*time.Millisecond) // <<<<<<<<<<<<<<<<<
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Sleep for 10 seconds
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				return task.ErrResult(ctx.Err())
			}
			return task.ErrResult(errors.New("invalid state, task should time out"))
		},
	})
	assert.NoError(t, err)

	// Wait for the task
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"warn","message":"task failed (%s): context deadline exceeded"}
{"level":"debug","message":"lock released%s"}
`)
	}, 5*time.Second, 100*time.Millisecond, logger.AllMessages())

	// Check etcd state after task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock",
  "error": "context deadline exceeded",
  "userError": {
    "name": "unknownError",
    "message": "Unknown error",
    "exceptionId": "test-service-%s"
  },
  "duration": %d
}
>>>>>
`, ignoredEtcdKeys)

	// Check logs
	logger.AssertJSONMessages(t, `
{"level":"info","message":"started task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"warn","message":"task failed (%s): context deadline exceeded","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock released \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
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
				Status: tracesdk.Status{Code: codes.Error, Description: "context deadline exceeded"},
				Attributes: []attribute.KeyValue{
					attribute.String("resource.name", "some.task"),
					attribute.String("task_id", "<dynamic>"),
					attribute.String("task_type", "some.task"),
					attribute.String("lock", "<dynamic>"),
					attribute.String("node", "node1"),
					attribute.String("created_at", "<dynamic>"),
					attribute.String("project_id", "123"),
					attribute.String("duration_sec", "<dynamic>"),
					attribute.String("finished_at", "<dynamic>"),
					attribute.Bool("is_success", false),
					attribute.Bool("is_application_error", true),
					attribute.String("error", "context deadline exceeded"),
					attribute.String("error_type", "deadline_exceeded"),
				},
				Events: []tracesdk.Event{
					{
						Name: "exception",
						Attributes: []attribute.KeyValue{
							attribute.String("exception.type", "context.deadlineExceededError"),
							attribute.String("exception.message", "context deadline exceeded"),
						},
					},
				},
			},
		},
		telemetry.WithSpanAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
			switch attr.Key {
			case "task_id", "lock", "created_at", "duration_sec", "finished_at":
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
								attribute.String("task_type", "some.task"),
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
								attribute.String("task_type", "some.task"),
								attribute.Bool("is_success", false),
								attribute.Bool("is_application_error", true),
								attribute.String("error_type", "deadline_exceeded"),
							),
						},
					},
				},
			},
		},
	)
}

func TestWorkerNodeShutdownDuringTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)

	lock := "my-lock"
	taskType := "some.task"
	tKey := task.Key{
		ProjectID: 123,
		TaskID:    task.ID("my-receiver/my-export/" + taskType),
	}

	logs := ioutil.NewAtomicWriter()
	tel := newTestTelemetryWithFilter(t)

	// Create node
	node1, d := createNode(t, ctx, etcdCfg, logs, tel, "node1")
	tel.Reset()
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	etcdhelper.ExpectModification(t, client, func() {
		_, err := node1.StartTask(ctx, task.Config{
			Key:  tKey,
			Type: taskType,
			Lock: lock,
			Context: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(ctx, time.Minute)
			},
			Operation: func(ctx context.Context, logger log.Logger) task.Result {
				defer close(taskDone)
				<-taskWork
				logger.Info(ctx, "some message from the task")
				return task.OkResult("some result")
			},
		})
		assert.NoError(t, err)
	})

	// Shutdown node
	shutdownDone := make(chan struct{})
	d.Process().Shutdown(ctx, errors.New("some reason"))
	go func() {
		defer close(shutdownDone)
		d.Process().WaitForShutdown()
	}()

	// Wait for task to finish
	time.Sleep(100 * time.Millisecond)
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Wait for shutdown
	select {
	case <-time.After(time.Second):
		assert.Fail(t, "timeout")
	case <-shutdownDone:
	}

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "type": "some.task",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "node1",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result",
  "duration": %d
}
>>>>>
`)

	// Check logs
	log.AssertJSONMessages(t, `
{"level":"info","message":"started task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"exiting (some reason)"}
{"level":"info","message":"received shutdown request","component":"task","node":"node1"}
{"level":"info","message":"waiting for \"1\" tasks to be finished","component":"task","node":"node1"}
{"level":"info","message":"some message from the task","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"task succeeded (%s): some result","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"debug","message":"lock released \"runtime/lock/task/my-lock\"","component":"task","task":"123/my-receiver/my-export/some.task/%s","node":"node1"}
{"level":"info","message":"closing etcd session: context canceled","component":"task.etcd.session","node":"node1"}
{"level":"info","message":"closed etcd session","component":"task.etcd.session","node":"node1"}
{"level":"info","message":"shutdown done","component":"task","node":"node1"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`, logs.String())
}

func newTestTelemetryWithFilter(t *testing.T) telemetry.ForTest {
	t.Helper()
	return telemetry.
		NewForTest(t).
		AddSpanFilter(func(ctx context.Context, spanName string, opts ...trace.SpanStartOption) bool {
			// Ignore etcd spans
			return !strings.HasPrefix(spanName, "etcd")
		}).
		AddMetricFilter(func(metric metricdata.Metrics) bool {
			// Ignore etcd metrics
			return !strings.HasPrefix(metric.Name, "rpc.")
		})
}

func createNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, logs io.Writer, tel telemetry.ForTest, nodeID string) (*task.Node, dependencies.Mocked) {
	t.Helper()
	d := createDeps(t, ctx, nodeID, etcdCfg, logs, tel)
	node, err := task.NewNode(nodeID, "test-service-", d, task.NewNodeConfig())
	require.NoError(t, err)
	d.DebugLogger().Truncate()
	return node, d
}

type taskNodeDeps struct {
	dependencies.Mocked
	dependencies.DistributionScope
}

func createDeps(t *testing.T, ctx context.Context, nodeID string, etcdCfg etcdclient.Config, logs io.Writer, tel telemetry.ForTest) *taskNodeDeps {
	t.Helper()

	mock := dependencies.NewMocked(
		t,
		ctx,
		dependencies.WithTelemetry(tel),
		dependencies.WithEtcdConfig(etcdCfg),
	)

	d := &taskNodeDeps{
		Mocked:            mock,
		DistributionScope: dependencies.NewDistributionScope(nodeID, distribution.NewConfig(), mock),
	}

	// Connect logs output
	if logs != nil {
		d.DebugLogger().ConnectTo(logs)
	}

	return d
}

func finishTaskAndWait(t *testing.T, client *etcd.Client, taskWork, taskDone chan struct{}) {
	t.Helper()

	// Wait for update of the task in etcd
	etcdhelper.ExpectModification(t, client, func() {
		// Finish work in the task
		close(taskWork)

		// Wait for goroutine
		select {
		case <-time.After(time.Second):
			assert.Fail(t, "timeout")
		case <-taskDone:
		}
	})
}
