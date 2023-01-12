package task_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestSuccessfulTask(t *testing.T) {
	t.Parallel()

	taskCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exportKey := exportKeyForTest()
	lockName := "my-lock"

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	logs := ioutil.NewBufferedWriter()

	// Create nodes
	node1, _ := createNode(t, etcdNamespace, logs, "node1")
	node2, _ := createNode(t, etcdNamespace, logs, "node2")
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	assert.NoError(t, node1.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (1)")
		return "some result (1)", nil
	}))
	assert.NoError(t, node2.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
		assert.Fail(t, "should not be called")
		return "", nil
	}))

	// Check etcd state during task
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/lock/task/00000123/my-receiver/my-export-1/my-lock (lease=%s)
-----
node1
>>>>>

<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock"
}
>>>>>
`)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after task
	etcdhelper.AssertKVs(t, client, `
<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock",
  "result": "some result (1)",
  "duration": %d
}
>>>>>
`)

	// Start another task with the same lock (lock is free)
	taskWork = make(chan struct{})
	taskDone = make(chan struct{})
	assert.NoError(t, node2.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (2)")
		return "some result (2)", nil
	}))

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after second task
	etcdhelper.AssertKVs(t, client, `
<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock",
  "result": "some result (1)",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node2",
  "lock": "my-lock",
  "result": "some result (2)",
  "duration": %d
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
[node1][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node1][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node1][tasks][taskId=%s]INFO  lock "my-lock" acquired
[node2][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node2][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node2][tasks][taskId=%s]INFO  task ignored, the lock "my-lock" is in use
[node1][tasks][taskId=%s]INFO  some message from the task (1)
[node1][tasks][taskId=%s]INFO  task succeeded (%s): some result (1)
[node1][tasks][taskId=%s]INFO  lock "my-lock" released
[node2][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node2][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node2][tasks][taskId=%s]INFO  lock "my-lock" acquired
[node2][tasks][taskId=%s]INFO  some message from the task (2)
[node2][tasks][taskId=%s]INFO  task succeeded (%s): some result (2)
[node2][tasks][taskId=%s]INFO  lock "my-lock" released
`, logs.String())
}

func TestFailedTask(t *testing.T) {
	t.Parallel()

	taskCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exportKey := exportKeyForTest()
	lockName := "my-lock"

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	logs := ioutil.NewBufferedWriter()

	// Create nodes
	node1, _ := createNode(t, etcdNamespace, logs, "node1")
	node2, _ := createNode(t, etcdNamespace, logs, "node2")
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	assert.NoError(t, node1.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (1)")
		return "", errors.New("some error (1)")
	}))
	assert.NoError(t, node2.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
		assert.Fail(t, "should not be called")
		return "", nil
	}))

	// Check etcd state during task
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/lock/task/00000123/my-receiver/my-export-1/my-lock (lease=%s)
-----
node1
>>>>>

<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock"
}
>>>>>
`)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after task
	etcdhelper.AssertKVs(t, client, `
<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock",
  "error": "some error (1)",
  "duration": %d
}
>>>>>
`)

	// Start another task with the same lock (lock is free)
	taskWork = make(chan struct{})
	taskDone = make(chan struct{})
	assert.NoError(t, node2.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (2)")
		return "", errors.New("some error (2)")
	}))

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after second task
	etcdhelper.AssertKVs(t, client, `
<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock",
  "error": "some error (1)",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node2",
  "lock": "my-lock",
  "error": "some error (2)",
  "duration": %d
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
[node1][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node1][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node1][tasks][taskId=%s]INFO  lock "my-lock" acquired
[node2][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node2][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node2][tasks][taskId=%s]INFO  task ignored, the lock "my-lock" is in use
[node1][tasks][taskId=%s]INFO  some message from the task (1)
[node1][tasks][taskId=%s]WARN  task failed (%s): some error (1)
[node1][tasks][taskId=%s]INFO  lock "my-lock" released
[node2][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node2][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node2][tasks][taskId=%s]INFO  lock "my-lock" acquired
[node2][tasks][taskId=%s]INFO  some message from the task (2)
[node2][tasks][taskId=%s]WARN  task failed (%s): some error (2)
[node2][tasks][taskId=%s]INFO  lock "my-lock" released
`, logs.String())
}

func TestWorkerNodeShutdownDuringTask(t *testing.T) {
	t.Parallel()

	taskCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exportKey := exportKeyForTest()
	lockName := "my-lock"

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	logs := ioutil.NewBufferedWriter()

	// Create node
	node1, d := createNode(t, etcdNamespace, logs, "node1")
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, node1.StartTask(taskCtx, exportKey, lockName, func(_ context.Context, logger log.Logger) (string, error) {
			defer close(taskDone)
			<-taskWork
			logger.Info("some message from the task")
			return "some result", nil
		}))
	})

	// Shutdown node
	shutdownDone := make(chan struct{})
	d.Process().Shutdown(errors.New("some reason"))
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
	etcdhelper.AssertKVs(t, client, `
<<<<<
task/00000123/my-receiver/my-export-1/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export-1",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "my-lock",
  "result": "some result",
  "duration": %d
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
[node1][tasks][taskId=%s]INFO  new task, key "00000123/my-receiver/my-export-1/%s"
[node1][tasks][taskId=%s]INFO  acquiring lock "my-lock"
[node1][tasks][taskId=%s]INFO  lock "my-lock" acquired
[node1]INFO  exiting (some reason)
[node1][tasks]INFO  received shutdown request
[node1][tasks]INFO  waiting for "1" tasks to be finished
[node1][tasks][taskId=%s]INFO  some message from the task
[node1][tasks][taskId=%s]INFO  task succeeded (%s): some result
[node1][tasks][taskId=%s]INFO  lock "my-lock" released
[node1][tasks]INFO  shutdown done
[node1][tasks][etcd-session]INFO  closing etcd session
[node1][tasks][etcd-session]INFO  closed etcd session | %s
[node1]INFO  exited
`, logs.String())
}

func exportKeyForTest() key.ExportKey {
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export-1"}
	return exportKey
}

func createNode(t *testing.T, etcdNamespace string, logs io.Writer, nodeName string) (*task.Node, dependencies.Mocked) {
	t.Helper()
	d := createDeps(t, etcdNamespace, logs, nodeName)
	node, err := task.NewNode(d)
	assert.NoError(t, err)
	return node, d
}

func createDeps(t *testing.T, etcdNamespace string, logs io.Writer, nodeName string) bufferDependencies.Mocked {
	t.Helper()
	d := bufferDependencies.NewMockedDeps(
		t,
		dependencies.WithUniqueID(nodeName),
		dependencies.WithLoggerPrefix(fmt.Sprintf("[%s]", nodeName)),
		dependencies.WithEtcdNamespace(etcdNamespace),
	)
	if logs != nil {
		d.DebugLogger().ConnectTo(logs)
	}
	d.DebugLogger().ConnectTo(testhelper.VerboseStdout())
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
