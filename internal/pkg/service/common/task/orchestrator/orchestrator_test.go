package orchestrator_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type testResource struct {
	ReceiverKey key.ReceiverKey
	ID          string
}

func (v testResource) GetReceiverKey() key.ReceiverKey {
	return v.ReceiverKey
}

func (v testResource) String() string {
	return v.ReceiverKey.String() + "/" + v.ID
}

func TestOrchestrator(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCredentials := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCredentials)

	d1 := dependencies.NewMocked(t,
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdCredentials(etcdCredentials),
		dependencies.WithEnabledOrchestrator(),
		dependencies.WithUniqueID("node1"),
	)
	d2 := dependencies.NewMocked(t,
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdCredentials(etcdCredentials),
		dependencies.WithEnabledOrchestrator(),
		dependencies.WithUniqueID("node2"),
	)
	node1 := orchestrator.NewNode(d1)
	node2 := orchestrator.NewNode(d2)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	pfx := etcdop.NewTypedPrefix[testResource]("my/prefix", serde.NewJSON(validator.New().Validate))

	// Orchestrator config
	config := orchestrator.Config[testResource]{
		Name: "some.task",
		Source: orchestrator.Source[testResource]{
			WatchPrefix:     pfx,
			RestartInterval: time.Minute,
		},
		DistributionKey: func(event etcdop.WatchEventT[testResource]) string {
			return event.Value.ReceiverKey.String()
		},
		Lock: func(event etcdop.WatchEventT[testResource]) string {
			// Define a custom lock name
			resource := event.Value
			return fmt.Sprintf(`%s/%s`, resource.ReceiverKey.String(), resource.ID)
		},
		TaskKey: func(event etcdop.WatchEventT[testResource]) task.Key {
			resource := event.Value
			return task.Key{
				ProjectID: resource.ReceiverKey.ProjectID,
				TaskID:    task.ID("my-receiver/some.task/" + resource.ID),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[testResource]) task.Fn {
			return func(ctx context.Context, logger log.Logger) task.Result {
				logger.InfoCtx(ctx, "message from the task")
				return task.OkResult(event.Value.ID)
			}
		},
	}

	// Create orchestrator per each node
	assert.NoError(t, <-node1.Start(config))
	assert.NoError(t, <-node2.Start(config))

	// Put some key to trigger the task
	assert.NoError(t, pfx.Key("key1").Put(testResource{ReceiverKey: receiverKey, ID: "ResourceID"}).Do(ctx, client))

	// Wait for task on the node 2
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"debug","message":"lock released%s"}`, d2.DebugLogger().AllMessages()) == nil
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	// Wait for  "not assigned" message form the node 1
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"debug","message":"not assigned%s"}`, d1.DebugLogger().AllMessages()) == nil
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d1.Process().Shutdown(ctx, errors.New("bye bye 1"))
	d1.Process().WaitForShutdown()
	d2.Process().Shutdown(ctx, errors.New("bye bye 2"))
	d2.Process().WaitForShutdown()

	expected := `
{"level":"info","message":"ready","component":"orchestrator","task":"some.task"}
{"level":"info","message":"assigned \"1000/my-receiver/some.task/ResourceID\"","component":"orchestrator","task":"some.task"}
{"level":"info","message":"started task","component":"task","task":"1000/my-receiver/some.task/ResourceID/%s"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/1000/my-receiver/ResourceID\"","component":"task","task":"1000/my-receiver/some.task/ResourceID/%s"}
{"level":"info","message":"message from the task","component":"task","task":"1000/my-receiver/some.task/ResourceID/%s"}
{"level":"info","message":"task succeeded (%s): ResourceID","component":"task","task":"1000/my-receiver/some.task/ResourceID/%s"}
{"level":"debug","message":"lock released \"runtime/lock/task/1000/my-receiver/ResourceID\"","component":"task","task":"1000/my-receiver/some.task/ResourceID/%s"}
`
	log.AssertJSONMessages(t, expected, d2.DebugLogger().AllMessages())

	expected = `
{"level":"info","message":"ready","component":"orchestrator","task":"some.task"}
{"level":"debug","message":"not assigned \"1000/my-receiver/some.task/ResourceID\", distribution key \"1000/my-receiver\"","component":"orchestrator","task":"some.task"}
`
	log.AssertJSONMessages(t, expected, d1.DebugLogger().AllMessages())
}

func TestOrchestrator_StartTaskIf(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCredentials := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCredentials)

	d := dependencies.NewMocked(t,
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdCredentials(etcdCredentials),
		dependencies.WithUniqueID("node1"),
		dependencies.WithEnabledOrchestrator(),
	)
	node := orchestrator.NewNode(d)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	pfx := etcdop.NewTypedPrefix[testResource]("my/prefix", serde.NewJSON(validator.New().Validate))

	// Orchestrator config
	config := orchestrator.Config[testResource]{
		Name: "some.task",
		Source: orchestrator.Source[testResource]{
			WatchPrefix:     pfx,
			RestartInterval: time.Minute,
		},
		DistributionKey: func(event etcdop.WatchEventT[testResource]) string {
			return event.Value.ReceiverKey.String()
		},
		TaskKey: func(event etcdop.WatchEventT[testResource]) task.Key {
			resource := event.Value
			return task.Key{
				ProjectID: resource.ReceiverKey.ProjectID,
				TaskID:    task.ID("my-receiver/some.task/" + resource.ID),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		StartTaskIf: func(event etcdop.WatchEventT[testResource]) (string, bool) {
			if event.Value.ID == "GoodID" { // <<<<<<<<<<<<<<<<<<<<
				return "", true
			}
			return "StartTaskIf condition evaluated as false", false
		},
		TaskFactory: func(event etcdop.WatchEventT[testResource]) task.Fn {
			return func(ctx context.Context, logger log.Logger) task.Result {
				logger.InfoCtx(ctx, "message from the task")
				return task.OkResult(event.Value.ID)
			}
		},
	}

	assert.NoError(t, <-node.Start(config))
	assert.NoError(t, pfx.Key("key1").Put(testResource{ReceiverKey: receiverKey, ID: "BadID"}).Do(ctx, client))
	assert.NoError(t, pfx.Key("key2").Put(testResource{ReceiverKey: receiverKey, ID: "GoodID"}).Do(ctx, client))
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"debug","message":"lock released%s"}`, d.DebugLogger().AllMessages()) == nil
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d.Process().Shutdown(ctx, errors.New("bye bye 1"))
	d.Process().WaitForShutdown()

	expected := `
{"level":"info","message":"ready","component":"orchestrator","task":"some.task"}
{"level":"debug","message":"skipped \"1000/my-receiver/some.task/BadID\", StartTaskIf condition evaluated as false","component":"orchestrator","task":"some.task"}
{"level":"info","message":"assigned \"1000/my-receiver/some.task/GoodID\"","component":"orchestrator","task":"some.task"}
{"level":"info","message":"started task","component":"task","task":"1000/my-receiver/some.task/GoodID/%s"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/1000/my-receiver/some.task/GoodID\"","component":"task","task":"1000/my-receiver/some.task/GoodID/%s"}
{"level":"info","message":"message from the task","component":"task","task":"1000/my-receiver/some.task/GoodID/%s"}
{"level":"info","message":"task succeeded (%s): GoodID","component":"task","task":"1000/my-receiver/some.task/GoodID/%s"}
{"level":"debug","message":"lock released \"runtime/lock/task/1000/my-receiver/some.task/GoodID\"","component":"task","task":"1000/my-receiver/some.task/GoodID/%s"}
`
	log.AssertJSONMessages(t, expected, d.DebugLogger().AllMessages())
}

func TestOrchestrator_RestartInterval(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCredentials := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCredentials)

	restartInterval := time.Millisecond
	clk := clock.NewMock()
	d := dependencies.NewMocked(t,
		dependencies.WithCtx(ctx),
		dependencies.WithClock(clk),
		dependencies.WithEtcdCredentials(etcdCredentials),
		dependencies.WithUniqueID("node1"),
		dependencies.WithEnabledOrchestrator(),
	)
	node := orchestrator.NewNode(d)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	pfx := etcdop.NewTypedPrefix[testResource]("my/prefix", serde.NewJSON(validator.New().Validate))

	// Orchestrator config
	config := orchestrator.Config[testResource]{
		Name: "some.task",
		Source: orchestrator.Source[testResource]{
			WatchPrefix:     pfx,
			RestartInterval: restartInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[testResource]) string {
			return event.Value.ReceiverKey.String()
		},
		TaskKey: func(event etcdop.WatchEventT[testResource]) task.Key {
			resource := event.Value
			return task.Key{
				ProjectID: resource.ReceiverKey.ProjectID,
				TaskID:    task.ID("my-receiver/some.task/" + resource.ID),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			// Each orchestrator task must have a deadline.
			return context.WithTimeout(ctx, time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[testResource]) task.Fn {
			return func(ctx context.Context, logger log.Logger) task.Result {
				logger.InfoCtx(ctx, "message from the task")
				return task.OkResult(event.Value.ID)
			}
		},
	}

	// Create orchestrator per each node
	assert.NoError(t, <-node.Start(config))

	// Put some key to trigger the task
	assert.NoError(t, pfx.Key("key1").Put(testResource{ReceiverKey: receiverKey, ID: "ResourceID1"}).Do(ctx, client))
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"debug","message":"lock released%s"}`, d.DebugLogger().AllMessages()) == nil
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	d.DebugLogger().Truncate()

	// 3x restart interval
	clk.Add(restartInterval)
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"debug","message":"restart"}`, d.DebugLogger().AllMessages()) == nil
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	// Put some key to trigger the task
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"debug","message":"lock released%s"}`, d.DebugLogger().AllMessages()) == nil
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	expected := `
{"level":"debug","message":"restart","component":"orchestrator","task":"some.task"}
{"level":"info","message":"assigned \"1000/my-receiver/some.task/ResourceID1\"","component":"orchestrator","task":"some.task"}
{"level":"info","message":"started task","component":"task","task":"1000/my-receiver/some.task/ResourceID1/%s"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/1000/my-receiver/some.task/ResourceID1\"","component":"task","task":"1000/my-receiver/some.task/ResourceID1/%s"}
{"level":"info","message":"message from the task","component":"task","task":"1000/my-receiver/some.task/ResourceID1/%s"}
{"level":"info","message":"task succeeded (0s): ResourceID1","component":"task","task":"1000/my-receiver/some.task/ResourceID1/%s"}
{"level":"debug","message":"lock released \"runtime/lock/task/1000/my-receiver/some.task/ResourceID1\"","component":"task","task":"1000/my-receiver/some.task/ResourceID1/%s"}
`
	log.AssertJSONMessages(t, expected, d.DebugLogger().AllMessages())
}
