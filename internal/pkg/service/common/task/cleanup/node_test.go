package cleanup_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	tmplDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d := tmplDependencies.NewMockedDeps(t, dependencies.WithEtcdNamespace(etcdNamespace))
	schema := d.Schema()

	node := cleanup.NewNode(d, d.Logger().AddPrefix("[cleanup]"))

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
	assert.NoError(t, schema.Tasks().ByKey(taskKey1).Put(task1).Do(ctx, client))

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
	assert.NoError(t, schema.Tasks().ByKey(taskKey2).Put(task2).Do(ctx, client))

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
	assert.NoError(t, schema.Tasks().ByKey(taskKey3).Put(task3).Do(ctx, client))

	// Run the cleanup
	assert.NoError(t, node.Check(ctx))

	// Shutdown - wait for tasks
	d.Process().Shutdown(errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
%A
[task][00000001/tasks.cleanup/%s]INFO  started task
[task][00000001/tasks.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/00000001/tasks.cleanup"
%A
[task][00000001/tasks.cleanup/%s]DEBUG  deleted task "00000123/some.task/2006-01-02T08:04:05.000Z_abcdef"
[task][00000001/tasks.cleanup/%s]DEBUG  deleted task "00000456/other.task/2006-01-02T08:04:05.000Z_ghijkl"
[task][00000001/tasks.cleanup/%s]INFO  deleted "2" tasks
[task][00000001/tasks.cleanup/%s]INFO  task succeeded (%s): deleted 2 tasks
[task][00000001/tasks.cleanup/%s]DEBUG  lock released "runtime/lock/task/00000001/tasks.cleanup"
%A
`, d.DebugLogger().AllMessages())

	// Check keys
	etcdhelper.AssertKVsString(t, client, `
<<<<<
task/00000789/third.task/2006-01-02T08:04:05.000Z_mnopqr
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
task/00000001/tasks.cleanup/%s
-----
{
  "projectId": 1,
  "taskId": "tasks.cleanup/%s",
  "type": "tasks.cleanup",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "%s",
  "lock": "runtime/lock/task/00000001/tasks.cleanup",
  "result": "deleted 2 tasks",
  "duration": %d
}
>>>>>
`)
}
