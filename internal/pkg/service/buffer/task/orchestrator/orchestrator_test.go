package orchestrator_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
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

	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d1 := bufferDependencies.NewMockedDeps(t,
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithUniqueID("node1"),
	)
	d2 := bufferDependencies.NewMockedDeps(t,
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithUniqueID("node2"),
	)

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
		TaskFactory: func(event etcdop.WatchEventT[testResource]) task.Task {
			return func(_ context.Context, logger log.Logger) (task.Result, error) {
				logger.Info("message from the task")
				return event.Value.ID, nil
			}
		},
	}

	// Create orchestrator per each node
	assert.NoError(t, <-orchestrator.Start(ctx, wg, d1, config))
	assert.NoError(t, <-orchestrator.Start(ctx, wg, d2, config))

	// Put some key to trigger the task
	assert.NoError(t, pfx.Key("key1").Put(testResource{ReceiverKey: receiverKey, ID: "ResourceID"}).Do(ctx, client))

	// Wait for task on the node 1
	assert.Eventually(t, func() bool {
		return strings.Contains(d1.DebugLogger().AllMessages(), "DEBUG  lock released")
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	// Wait for  "not assigned" message form the node 2
	assert.Eventually(t, func() bool {
		return strings.Contains(d2.DebugLogger().AllMessages(), "DEBUG  not assigned")
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d1.Process().Shutdown(errors.New("bye bye 1"))
	d1.Process().WaitForShutdown()
	d2.Process().Shutdown(errors.New("bye bye 2"))
	d2.Process().WaitForShutdown()

	wildcards.Assert(t, `
%A
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/some.task/ResourceID"
[task][%s]INFO  started task
[task][%s]DEBUG  lock acquired "runtime/lock/task/00001000/my-receiver/ResourceID"
[task][%s]INFO  message from the task
[task][%s]INFO  task succeeded (%s): ResourceID
[task][%s]DEBUG  lock released "runtime/lock/task/00001000/my-receiver/ResourceID"
%A
`, d1.DebugLogger().AllMessages())

	wildcards.Assert(t, `
%A
[orchestrator][some.task]INFO  ready
[orchestrator][some.task]DEBUG  not assigned "00001000/my-receiver/some.task/ResourceID", distribution key "00001000/my-receiver"
%A
`, d2.DebugLogger().AllMessages())
}

func TestOrchestrator_StartTaskIf(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d := bufferDependencies.NewMockedDeps(t,
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithUniqueID("node1"),
	)

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
		TaskFactory: func(event etcdop.WatchEventT[testResource]) task.Task {
			return func(_ context.Context, logger log.Logger) (task.Result, error) {
				logger.Info("message from the task")
				return event.Value.ID, nil
			}
		},
	}

	assert.NoError(t, <-orchestrator.Start(ctx, wg, d, config))
	assert.NoError(t, pfx.Key("key1").Put(testResource{ReceiverKey: receiverKey, ID: "BadID"}).Do(ctx, client))
	assert.NoError(t, pfx.Key("key2").Put(testResource{ReceiverKey: receiverKey, ID: "GoodID"}).Do(ctx, client))
	assert.Eventually(t, func() bool {
		return strings.Contains(d.DebugLogger().AllMessages(), "DEBUG  lock released")
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d.Process().Shutdown(errors.New("bye bye 1"))
	d.Process().WaitForShutdown()

	wildcards.Assert(t, `
%A
[orchestrator][some.task]INFO  ready
[orchestrator][some.task]DEBUG  skipped "00001000/my-receiver/some.task/BadID", StartTaskIf condition evaluated as false
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/some.task/GoodID"
[task][00001000/my-receiver/some.task/GoodID/%s]INFO  started task
[task][00001000/my-receiver/some.task/GoodID/%s]DEBUG  lock acquired "runtime/lock/task/00001000/my-receiver/some.task/GoodID"
[task][00001000/my-receiver/some.task/GoodID/%s]INFO  message from the task
[task][00001000/my-receiver/some.task/GoodID/%s]INFO  task succeeded (%s): GoodID
[task][00001000/my-receiver/some.task/GoodID/%s]DEBUG  lock released "runtime/lock/task/00001000/my-receiver/some.task/GoodID"
%A
`, d.DebugLogger().AllMessages())
}

func TestOrchestrator_RestartInterval(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	restartInterval := time.Millisecond
	clk := clock.NewMock()
	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d := bufferDependencies.NewMockedDeps(t,
		dependencies.WithCtx(ctx),
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithUniqueID("node1"),
	)

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
		TaskFactory: func(event etcdop.WatchEventT[testResource]) task.Task {
			return func(_ context.Context, logger log.Logger) (task.Result, error) {
				logger.Info("message from the task")
				return event.Value.ID, nil
			}
		},
	}

	// Create orchestrator per each node
	assert.NoError(t, <-orchestrator.Start(ctx, wg, d, config))

	// Put some key to trigger the task
	assert.NoError(t, pfx.Key("key1").Put(testResource{ReceiverKey: receiverKey, ID: "ResourceID1"}).Do(ctx, client))
	assert.Eventually(t, func() bool {
		return strings.Contains(d.DebugLogger().AllMessages(), "DEBUG  lock released")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	d.DebugLogger().Truncate()

	// 3x restart interval
	clk.Add(restartInterval)
	assert.Eventually(t, func() bool {
		return strings.Contains(d.DebugLogger().AllMessages(), "DEBUG  restart")
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	// Put some key to trigger the task
	assert.Eventually(t, func() bool {
		return strings.Contains(d.DebugLogger().AllMessages(), "DEBUG  lock released")
	}, 5*time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d.Process().Shutdown(errors.New("bye bye"))
	d.Process().WaitForShutdown()

	wildcards.Assert(t, `
[orchestrator][some.task]DEBUG  restart
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/some.task/ResourceID1"
[task][00001000/my-receiver/some.task/ResourceID1/%s]INFO  started task
[task][00001000/my-receiver/some.task/ResourceID1/%s]DEBUG  lock acquired "runtime/lock/task/00001000/my-receiver/some.task/ResourceID1"
[task][00001000/my-receiver/some.task/ResourceID1/%s]INFO  message from the task
[task][00001000/my-receiver/some.task/ResourceID1/%s]INFO  task succeeded (0s): ResourceID1
[task][00001000/my-receiver/some.task/ResourceID1/%s]DEBUG  lock released "runtime/lock/task/00001000/my-receiver/some.task/ResourceID1"
%A
`, d.DebugLogger().AllMessages())
}
