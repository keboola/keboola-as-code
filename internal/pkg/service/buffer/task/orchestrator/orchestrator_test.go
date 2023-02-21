package orchestrator_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
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
	d1 := bufferDependencies.NewMockedDeps(t, dependencies.WithCtx(ctx), dependencies.WithEtcdNamespace(etcdNamespace), dependencies.WithUniqueID("node1"))
	d2 := bufferDependencies.NewMockedDeps(t, dependencies.WithCtx(ctx), dependencies.WithEtcdNamespace(etcdNamespace), dependencies.WithUniqueID("node2"))

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	pfx := etcdop.NewTypedPrefix[testResource]("my/prefix", serde.NewJSON(validator.New().Validate))

	// Orchestrator config
	config := orchestrator.Config[testResource]{
		Prefix:         pfx,
		ReSyncInterval: time.Minute,
		TaskType:       "some.task",
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
[orchestrator][some.task]INFO  ready
[distribution]INFO  found a new node "node2"
[orchestrator][some.task]INFO  restart: distribution changed: found a new node "node2"
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/ResourceID"
[task][%s]INFO  started task "00001000/my-receiver/some.task/%s"
[task][%s]DEBUG  lock acquired "runtime/lock/task/some.task/00001000/my-receiver/ResourceID"
[task][%s]INFO  message from the task
[task][%s]INFO  task succeeded (%s): ResourceID
[task][%s]DEBUG  lock released "runtime/lock/task/some.task/00001000/my-receiver/ResourceID"
%A
`, d1.DebugLogger().AllMessages())

	wildcards.Assert(t, `
%A
[orchestrator][some.task]INFO  ready
[orchestrator][some.task]DEBUG  not assigned "00001000/my-receiver/ResourceID"
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
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithCtx(ctx), dependencies.WithEtcdNamespace(etcdNamespace), dependencies.WithUniqueID("node1"))

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	pfx := etcdop.NewTypedPrefix[testResource]("my/prefix", serde.NewJSON(validator.New().Validate))

	// Orchestrator config
	config := orchestrator.Config[testResource]{
		Prefix:         pfx,
		ReSyncInterval: time.Minute,
		TaskType:       "some.task",
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
[orchestrator][some.task]DEBUG  skipped "00001000/my-receiver/BadID", StartTaskIf condition evaluated as false
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/GoodID"
[task][some.task/%s]INFO  started task "00001000/my-receiver/some.task/%s"
[task][some.task/%s]DEBUG  lock acquired "runtime/lock/task/some.task/00001000/my-receiver/GoodID"
[task][some.task/%sINFO  message from the task
[task][some.task/%s]INFO  task succeeded (%s): GoodID
[task][some.task/%s]DEBUG  lock released "runtime/lock/task/some.task/00001000/my-receiver/GoodID"
%A
`, d.DebugLogger().AllMessages())
}
