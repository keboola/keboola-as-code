package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

// TestSliceCloseTask - the worker closes the slice only after it is not used by any API node.
func TestSliceCloseTask(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{dependencies.WithClock(clk), dependencies.WithEtcdNamespace(etcdNamespace)}

	// Start API node
	apiDeps1 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node-1"))...)
	apiDeps1.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	apiNode := apiDeps1.WatcherAPINode()

	// Some other API node is also running
	apiDeps2 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node-2"))...)
	_ = apiDeps2.WatcherAPINode()

	// Start worker node
	workerDeps := bufferDependencies.NewMockedDeps(t, opts...)
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err := service.New(
		workerDeps,
		service.WithCheckConditions(false),
		service.WithCloseSlices(true),
		service.WithUploadSlices(false),
		service.WithRetryFailedSlices(false),
		service.WithCloseFiles(false),
		service.WithImportFiles(false),
		service.WithRetryFailedFiles(false),
	)
	assert.NoError(t, err)

	// Create receivers, exports and records
	str := apiDeps1.Store()
	emptySliceKey := createExport(t, "my-receiver-1", "my-export-1", ctx, clk, client, str, nil)
	clk.Add(time.Minute)
	notEmptySliceKey := createExport(t, "my-receiver-2", "my-export-2", ctx, clk, client, str, nil)
	clk.Add(time.Minute)
	createRecords(t, ctx, clk, apiDeps1, notEmptySliceKey.ReceiverKey, 1, 1)
	createRecords(t, ctx, clk, apiDeps2, notEmptySliceKey.ReceiverKey, 2, 2)

	// NOW = slice.closingAt = task.createdAt = 0001-01-01T00:01:01Z
	clk.Add(time.Minute)

	// Truncate init logs
	apiDeps1.DebugLogger().Truncate()
	workerDeps.DebugLogger().Truncate()

	// The receiver, in the current state, is twice used by some requests, in the API node
	_, found, unlockR1 := apiNode.GetReceiver(emptySliceKey.ReceiverKey)
	assert.True(t, found)
	_, found, unlockR2 := apiNode.GetReceiver(emptySliceKey.ReceiverKey)
	assert.True(t, found)
	_, found, unlockR3 := apiNode.GetReceiver(notEmptySliceKey.ReceiverKey)
	assert.True(t, found)
	workerDeps.DebugLogger().Info("---> locked")
	apiDeps1.DebugLogger().Info("---> locked")

	// Close the empty slice (mapping has been changed or upload conditions are met)
	emptySlice, err := str.GetSlice(ctx, emptySliceKey)
	assert.NoError(t, err)
	header := etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.SetSliceState(ctx, &emptySlice, slicestate.Closing))
	})
	assert.Eventually(t, func() bool {
		return apiNode.StateRev() >= header.Revision
	}, time.Second, 10*time.Millisecond, "timeout")
	assert.Eventually(t, func() bool {
		return workerDeps.WatcherWorkerNode().ListenersCount() == 1
	}, time.Second, 10*time.Millisecond, "timeout")

	// Close the not empty slice (mapping has been changed or upload conditions are met)
	notEmptySlice, err := str.GetSlice(ctx, notEmptySliceKey)
	assert.NoError(t, err)
	header = etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.SetSliceState(ctx, &notEmptySlice, slicestate.Closing))
	})
	assert.Eventually(t, func() bool {
		return apiNode.StateRev() >= header.Revision
	}, time.Second, 10*time.Millisecond, "timeout")
	assert.Eventually(t, func() bool {
		return workerDeps.WatcherWorkerNode().ListenersCount() == 2
	}, time.Second, 10*time.Millisecond, "timeout")

	// NOW = slice.uploadingAt = task.finishedAt = 0001-01-01T00:01:31Z
	clk.Add(30 * time.Second)

	// Requests which were writing to the closing slice are completed.
	// Worker can switch the slice from the closing to the uploading state.
	workerDeps.DebugLogger().Info("---> unlocked")
	apiDeps1.DebugLogger().Info("---> unlocked")
	unlockR1()
	unlockR2()
	unlockR3()
	assert.Eventually(t, func() bool {
		return workerDeps.TaskWorkerNode().TasksCount() == 0
	}, time.Second, 10*time.Millisecond, "timeout")

	// Shutdown API node and worker
	apiDeps1.Process().Shutdown(errors.New("bye bye API"))
	apiDeps1.Process().WaitForShutdown()
	apiDeps2.Process().Shutdown(errors.New("bye bye API"))
	apiDeps2.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()

	// Get tasks
	receiver1Tasks, err := workerDeps.Schema().Tasks().InReceiver(emptySliceKey.ReceiverKey).GetAll().Do(ctx, client).All()
	assert.NoError(t, err)
	assert.Len(t, receiver1Tasks, 1)
	receiver2Tasks, err := workerDeps.Schema().Tasks().InReceiver(notEmptySlice.ReceiverKey).GetAll().Do(ctx, client).All()
	assert.NoError(t, err)
	assert.Len(t, receiver2Tasks, 1)
	task1 := receiver1Tasks[0].Value
	task2 := receiver2Tasks[0].Value

	// Check API logs
	wildcards.Assert(t, `
[api][watcher]INFO  locked revision "%s"
INFO  ---> locked
[api][watcher]INFO  deleted slice/active/opened/writing/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
[api][watcher]INFO  state updated to the revision "%s"
[api][watcher]INFO  deleted slice/active/opened/writing/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z
[api][watcher]INFO  state updated to the revision "%s"
INFO  ---> unlocked
[api][watcher]INFO  unlocked revision "%s"
[api][watcher]INFO  reported revision "%s"
INFO  exiting (bye bye API)
[api][watcher]INFO  received shutdown request
[api][watcher]INFO  shutdown done
[api][watcher][etcd-session]INFO  closing etcd session
[api][watcher][etcd-session]INFO  closed etcd session | %s
[stats]INFO  received shutdown request
[stats]INFO  shutdown done
INFO  exited
`, apiDeps1.DebugLogger().AllMessages())

	// Check worker logs
	wildcards.Assert(t, `
INFO  ---> locked
[task][slice.close/%s]INFO  started task "00000123/my-receiver-1/slice.close/%s"
[task][slice.close/%s]DEBUG  lock acquired "runtime/lock/task/slice.close/00000123/my-receiver-1/%s"
[task][slice.close/%s]INFO  waiting until all API nodes switch to a revision >= %d
INFO  ---> unlocked
[task][slice.close/%s]INFO  task succeeded (30s): slice closed
[task][slice.close/%s]DEBUG  lock released "runtime/lock/task/slice.close/00000123/my-receiver-1/%s"
`, strhelper.FilterLines(`^(INFO  --->)|(\[task\]\[`+task1.ID()+`\])`, workerDeps.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
INFO  ---> locked
[task][slice.close/%a]INFO  started task "00000123/my-receiver-2/slice.close/%s"
[task][slice.close/%s]DEBUG  lock acquired "runtime/lock/task/slice.close/00000123/my-receiver-2/%s"
[task][slice.close/%s]INFO  waiting until all API nodes switch to a revision >= %d
INFO  ---> unlocked
[task][slice.close/%s]INFO  task succeeded (30s): slice closed
[task][slice.close/%s]DEBUG  lock released "runtime/lock/task/slice.close/00000123/my-receiver-2/%s"
`, strhelper.FilterLines(`^(INFO  --->)|(\[task\]\[`+task2.ID()+`\])`, workerDeps.DebugLogger().AllMessages()))

	// Check etcd state
	assertStateAfterClose(t, client)

	// After deleting the receivers, the database should remain empty
	assert.NoError(t, str.DeleteReceiver(ctx, emptySlice.ReceiverKey))
	assert.NoError(t, str.DeleteReceiver(ctx, notEmptySlice.ReceiverKey))
	etcdhelper.AssertKVs(t, client, "")
}

func assertStateAfterClose(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVs(t, client, `
<<<<<
config/export/00000123/my-receiver-1/my-export-1
-----
%A
>>>>>

<<<<<
config/export/00000123/my-receiver-2/my-export-2
-----
%A
>>>>>

<<<<<
config/mapping/revision/00000123/my-receiver-1/my-export-1/00000001
-----
%A
>>>>>

<<<<<
config/mapping/revision/00000123/my-receiver-2/my-export-2/00000001
-----
%A
>>>>>

<<<<<
config/receiver/00000123/my-receiver-1
-----
%A
>>>>>

<<<<<
config/receiver/00000123/my-receiver-2
-----
%A
>>>>>

<<<<<
file/opened/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z
-----
%A
>>>>>

<<<<<
file/opened/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z
-----
%A
>>>>>

<<<<<
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:02.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:02.000Z,1.2.3.4,"{""key"":""value001""}","{""Content-Type"":""application/json""}","""---value001---"""
>>>>>

<<<<<
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:03.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:03.000Z,1.2.3.4,"{""key"":""value002""}","{""Content-Type"":""application/json""}","""---value002---"""
>>>>>

<<<<<
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:04.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:04.000Z,1.2.3.4,"{""key"":""value003""}","{""Content-Type"":""application/json""}","""---value003---"""
>>>>>

<<<<<
runtime/last/record/id/00000123/my-receiver-2/my-export-2
-----
3
>>>>>

<<<<<
secret/export/token/00000123/my-receiver-1/my-export-1
-----
%A
>>>>>

<<<<<
secret/export/token/00000123/my-receiver-2/my-export-2
-----
%A
>>>>>

<<<<<
slice/active/closed/uploading/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
-----
{
%A
  "state": "active/closed/uploading",
%A
  "sliceNumber": 1,
  "closingAt": "0001-01-01T00:03:04.000Z",
  "uploadingAt": "0001-01-01T00:03:34.000Z",
  "isEmpty": true
%A
>>>>>

<<<<<
slice/active/closed/uploading/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z
-----
{
%A
  "state": "active/closed/uploading",
%A
  "sliceNumber": 1,
  "closingAt": "0001-01-01T00:03:04.000Z",
  "uploadingAt": "0001-01-01T00:03:34.000Z",
  "statistics": {
    "lastRecordAt": "0001-01-01T00:02:04.000Z",
    "recordsCount": 3,
    "recordsSize": "396B",
    "bodySize": "54B"
  },
  "idRange": {
    "start": 1,
    "count": 3
  }
%A
>>>>>
`)
}
