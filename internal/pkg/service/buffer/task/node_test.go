package task_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestSuccessfulTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lock := "my-lock"
	taskKey := key.TaskKey{
		ProjectID: 123,
		TaskID:    "my-receiver/my-export/some.task",
	}

	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	logs := ioutil.NewAtomicWriter()

	// Create nodes
	node1, _ := createNode(t, etcdNamespace, logs, "node1")
	node2, _ := createNode(t, etcdNamespace, logs, "node2")
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	_, err := node1.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (task.Result, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (1)")
		return "some result (1)", nil
	}, task.WithLock(lock))
	assert.NoError(t, err)
	_, err = node2.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (task.Result, error) {
		assert.Fail(t, "should not be called")
		return "", nil
	}, task.WithLock(lock))
	assert.NoError(t, err)

	// Check etcd state during task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/lock/task/my-lock (lease)
-----
node1
>>>>>

<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock"
}
>>>>>
`)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result (1)",
  "duration": %d
}
>>>>>
`)

	// Start another task with the same lock (lock is free)
	taskWork = make(chan struct{})
	taskDone = make(chan struct{})
	_, err = node2.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (2)")
		return "some result (2)", nil
	}, task.WithLock(lock))
	assert.NoError(t, err)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after second task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result (1)",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node2",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result (2)",
  "duration": %d
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  started task
[node1][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock acquired "runtime/lock/task/my-lock"
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  task ignored, the lock "runtime/lock/task/my-lock" is in use
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  some message from the task (1)
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  task succeeded (%s): some result (1)
[node1][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock released "runtime/lock/task/my-lock"
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  started task
[node2][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock acquired "runtime/lock/task/my-lock"
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  some message from the task (2)
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  task succeeded (%s): some result (2)
[node2][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock released "runtime/lock/task/my-lock"
`, logs.String())
}

func TestFailedTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lock := "my-lock"
	taskKey := key.TaskKey{
		ProjectID: 123,
		TaskID:    "my-receiver/my-export/some.task",
	}
	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	logs := ioutil.NewAtomicWriter()

	// Create nodes
	node1, _ := createNode(t, etcdNamespace, logs, "node1")
	node2, _ := createNode(t, etcdNamespace, logs, "node2")
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	_, err := node1.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (1)")
		return "", errors.New("some error (1)")
	}, task.WithLock(lock))
	assert.NoError(t, err)
	_, err = node2.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (string, error) {
		assert.Fail(t, "should not be called")
		return "", nil
	}, task.WithLock(lock))
	assert.NoError(t, err)

	// Check etcd state during task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/lock/task/my-lock (lease)
-----
node1
>>>>>

<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock"
}
>>>>>
`)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock",
  "error": "some error (1)",
  "duration": %d
}
>>>>>
`)

	// Start another task with the same lock (lock is free)
	taskWork = make(chan struct{})
	taskDone = make(chan struct{})
	_, err = node2.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (string, error) {
		defer close(taskDone)
		<-taskWork
		logger.Info("some message from the task (2)")
		return "", errors.New("some error (2)")
	}, task.WithLock(lock))
	assert.NoError(t, err)

	// Wait for task to finish
	finishTaskAndWait(t, client, taskWork, taskDone)

	// Check etcd state after second task
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock",
  "error": "some error (1)",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node2",
  "lock": "runtime/lock/task/my-lock",
  "error": "some error (2)",
  "duration": %d
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  started task
[node1][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock acquired "runtime/lock/task/my-lock"
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  task ignored, the lock "runtime/lock/task/my-lock" is in use
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  some message from the task (1)
[node1][task][00000123/my-receiver/my-export/some.task/%s]WARN  task failed (%s): some error (1) [%s]
[node1][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock released "runtime/lock/task/my-lock"
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  started task
[node2][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock acquired "runtime/lock/task/my-lock"
[node2][task][00000123/my-receiver/my-export/some.task/%s]INFO  some message from the task (2)
[node2][task][00000123/my-receiver/my-export/some.task/%s]WARN  task failed (%s): some error (2) [%s]
[node2][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock released "runtime/lock/task/my-lock"
`, logs.String())
}

func TestWorkerNodeShutdownDuringTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lock := "my-lock"
	taskKey := key.TaskKey{
		ProjectID: 123,
		TaskID:    "my-receiver/my-export/some.task",
	}
	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	logs := ioutil.NewAtomicWriter()

	// Create node
	node1, d := createNode(t, etcdNamespace, logs, "node1")
	logs.Truncate()

	// Start a task
	taskWork := make(chan struct{})
	taskDone := make(chan struct{})
	etcdhelper.ExpectModification(t, client, func() {
		_, err := node1.StartTask(ctx, taskKey, func(ctx context.Context, logger log.Logger) (string, error) {
			defer close(taskDone)
			<-taskWork
			logger.Info("some message from the task")
			return "some result", nil
		}, task.WithLock(lock))
		assert.NoError(t, err)
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
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/00000123/my-receiver/my-export/some.task/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver/my-export/some.task/%s",
  "createdAt": "%s",
  "finishedAt": "%s",
  "workerNode": "node1",
  "lock": "runtime/lock/task/my-lock",
  "result": "some result",
  "duration": %d
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
[node1][task][%s]INFO  started task
[node1][task][%s]DEBUG  lock acquired "runtime/lock/task/my-lock"
[node1]INFO  exiting (some reason)
[node1][task]INFO  received shutdown request
[node1][task]INFO  waiting for "1" tasks to be finished
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  some message from the task
[node1][task][00000123/my-receiver/my-export/some.task/%s]INFO  task succeeded (%s): some result
[node1][task][00000123/my-receiver/my-export/some.task/%s]DEBUG  lock released "runtime/lock/task/my-lock"
[node1][task][etcd-session]INFO  closing etcd session
[node1][task][etcd-session]INFO  closed etcd session | %s
[node1][task]INFO  shutdown done
[node1]INFO  exited
`, logs.String())
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
