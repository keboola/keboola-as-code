package orchestrator_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type testResource struct {
	ExportKey key.ExportKey
	ID        string
}

func (v testResource) GetExportKey() key.ExportKey {
	return v.ExportKey
}

func (v testResource) String() string {
	return v.ExportKey.String() + "/" + v.ID
}

func TestOrchestrator(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d1 := bufferDependencies.NewMockedDeps(t, dependencies.WithCtx(ctx), dependencies.WithEtcdNamespace(etcdNamespace), dependencies.WithUniqueID("node1"))
	d2 := bufferDependencies.NewMockedDeps(t, dependencies.WithCtx(ctx), dependencies.WithEtcdNamespace(etcdNamespace), dependencies.WithUniqueID("node2"))

	exportKey := key.ExportKey{ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}, ExportID: "my-export"}
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
	assert.NoError(t, pfx.Key("key1").Put(testResource{ExportKey: exportKey, ID: "ResourceID"}).Do(ctx, client))

	// Wait for "not assigned" message form the node 1
	assert.Eventually(t, func() bool {
		return strings.Contains(d1.DebugLogger().AllMessages(), "DEBUG  not assigned")
	}, time.Second, 10*time.Millisecond, "timeout")

	// Wait for task on the node 2
	assert.Eventually(t, func() bool {
		return strings.Contains(d2.DebugLogger().AllMessages(), "DEBUG  lock released")
	}, time.Second, 10*time.Millisecond, "timeout")

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
[orchestrator][some.task]DEBUG  not assigned "00001000/my-receiver/my-export/ResourceID"
%A
`, d1.DebugLogger().AllMessages())

	wildcards.Assert(t, `
%A
[orchestrator][some.task]INFO  ready
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/my-export/ResourceID"
[task][%s]INFO  started task "00001000/my-receiver/my-export/some.task/%s"
[task][%s]DEBUG  lock acquired "runtime/lock/task/00001000/my-receiver/my-export/some.task/ResourceID"
[task][%s]INFO  message from the task
[task][%s]INFO  task succeeded (%s): ResourceID
[task][%s]DEBUG  lock released "runtime/lock/task/00001000/my-receiver/my-export/some.task/ResourceID"
%A
`, d2.DebugLogger().AllMessages())
}

func TestOrchestrator_StartTaskIf(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithCtx(ctx), dependencies.WithEtcdNamespace(etcdNamespace), dependencies.WithUniqueID("node1"))

	exportKey := key.ExportKey{ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}, ExportID: "my-export"}
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
	assert.NoError(t, pfx.Key("key1").Put(testResource{ExportKey: exportKey, ID: "BadID"}).Do(ctx, client))
	assert.NoError(t, pfx.Key("key2").Put(testResource{ExportKey: exportKey, ID: "GoodID"}).Do(ctx, client))
	assert.Eventually(t, func() bool {
		return strings.Contains(d.DebugLogger().AllMessages(), "DEBUG  lock released")
	}, time.Second, 10*time.Millisecond, "timeout")

	cancel()
	wg.Wait()
	d.Process().Shutdown(errors.New("bye bye 1"))
	d.Process().WaitForShutdown()

	wildcards.Assert(t, `
%A
[orchestrator][some.task]INFO  ready
[orchestrator][some.task]DEBUG  skipped "00001000/my-receiver/my-export/BadID", StartTaskIf condition evaluated as false
[orchestrator][some.task]INFO  assigned "00001000/my-receiver/my-export/GoodID"
[task][some.task/%s]INFO  started task "00001000/my-receiver/my-export/some.task/%s"
[task][some.task/%s]DEBUG  lock acquired "runtime/lock/task/00001000/my-receiver/my-export/some.task/GoodID"
[task][some.task/%sINFO  message from the task
[task][some.task/%s]INFO  task succeeded (%s): GoodID
[task][some.task/%s]DEBUG  lock released "runtime/lock/task/00001000/my-receiver/my-export/some.task/GoodID"
%A
`, d.DebugLogger().AllMessages())
}
