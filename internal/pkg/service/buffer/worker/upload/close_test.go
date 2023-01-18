package upload_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/upload"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestSliceCloseTask(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{dependencies.WithClock(clk), dependencies.WithEtcdNamespace(etcdNamespace)}
	str := bufferDependencies.NewMockedDeps(t, opts...).Store()

	// Create an export
	sliceKey := createExport(t, ctx, clk, client, str)

	// Start API node
	apiDeps := bufferDependencies.NewMockedDeps(t, opts...)
	apiDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	apiNode := apiDeps.WatcherAPINode()

	// Start worker node
	workerDeps := bufferDependencies.NewMockedDeps(t, opts...)
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err := upload.NewUploader(workerDeps, upload.WithCloseSlices(true), upload.WithUploadSlices(false))
	assert.NoError(t, err)

	// The receiver, in the current state, is twice used by some requests, in the API node
	_, found, unlockR1 := apiNode.GetReceiver(sliceKey.ReceiverKey)
	assert.True(t, found)
	_, found, unlockR2 := apiNode.GetReceiver(sliceKey.ReceiverKey)
	assert.True(t, found)

	// NOW = slice.closingAt = task.createdAt = 0001-01-01T00:01:01Z
	clk.Add(time.Minute)

	// Slice is closing (mapping has been changed or upload conditions are met)
	slice, err := str.GetSlice(ctx, sliceKey)
	assert.NoError(t, err)
	header := etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.SetSliceState(ctx, &slice, slicestate.Closing))
	})

	// Wait for sync of the Watcher in the API node
	assert.Eventually(t, func() bool {
		return apiDeps.WatcherAPINode().StateRev() == header.Revision
	}, time.Second, 10*time.Millisecond, "timeout")

	// Wait until the worker node start wait for the API nodes
	assert.Eventually(t, func() bool {
		return workerDeps.WatcherWorkerNode().ListenersCount() == 1
	}, time.Second, 10*time.Millisecond, "timeout")

	// NOW = slice.uploadingAt = task.finishedAt = 0001-01-01T00:01:31Z
	clk.Add(30 * time.Second)

	// Requests which were writing to the closing slice are completed.
	// Worker can switch the slice from the closing to the uploading state.
	workerDeps.DebugLogger().Info("---> first unlock")
	apiDeps.DebugLogger().Info("---> first unlock")
	unlockR1()
	workerDeps.DebugLogger().Info("---> second unlock")
	apiDeps.DebugLogger().Info("---> second unlock")
	unlockR2()

	// Wait for the Worker task to finish
	assert.Eventually(t, func() bool {
		return workerDeps.TaskWorkerNode().TasksCount() == 0
	}, time.Second, 10*time.Millisecond, "timeout")

	// Shutdown API node and worker
	apiDeps.Process().Shutdown(errors.New("bye bye API"))
	apiDeps.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()

	// Check etcd state
	etcdhelper.AssertKVs(t, client, `
%A
<<<<<
slice/uploading/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "state": "uploading",
  "mapping": {
    "projectId": 123,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": "in.c-bucket.table",
    "incremental": false,
    "columns": [
      {
        "type": "id",
        "name": "col01"
      },
      {
        "type": "datetime",
        "name": "col02"
      },
      {
        "type": "ip",
        "name": "col03"
      },
      {
        "type": "body",
        "name": "col04"
      },
      {
        "type": "headers",
        "name": "col05"
      },
      {
        "type": "template",
        "name": "col06",
        "language": "jsonnet",
        "content": "\"---\" + Body(\"key\") + \"---\""
      }
    ]
  },
  "sliceNumber": 1,
  "closingAt": "0001-01-01T00:01:01.000Z",
  "uploadingAt": "0001-01-01T00:01:31.000Z"
}
>>>>>

<<<<<
task/00000123/my-receiver/my-export/slice.close/0001-01-01T00:01:01.000Z_%c%c%c%c%c
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "slice.close",
  "createdAt": "0001-01-01T00:01:01.000Z",
  "randomId": "%s",
  "finishedAt": "0001-01-01T00:01:31.000Z",
  "workerNode": "%s",
  "lock": "slice.close/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "result": "slice closed",
  "duration": 30000000000
}
>>>>>
`)

	// Check logs
	wildcards.Assert(t, `
INFO  process unique id "%s"
[api][watcher][etcd-session]INFO  creating etcd session
[api][watcher][etcd-session]INFO  created etcd session | %s
[api][watcher]INFO  reported revision "1"
[api][watcher]INFO  state updated to the revision "%s"
[api][watcher]INFO  reported revision "%s"
[api][watcher]INFO  initialized | %s
[api][watcher]INFO  locked revision "%s"
[api][watcher]INFO  deleted slice/opened/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
[api][watcher]INFO  state updated to the revision "%s"
INFO  ---> first unlock
INFO  ---> second unlock
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

`, apiDeps.DebugLogger().AllMessages())

	wildcards.Assert(t, `
%A
[orchestrator][slice.close]INFO  ready
[orchestrator][slice.close]INFO  assigned "00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z"
[task][%s]INFO  started task "00000123/my-receiver/my-export/slice.close/0001-01-01T00:01:01.000Z_%s"
[task][%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver/my-export/slice.close/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z"
[task][%s]INFO  waiting until all API nodes switch to a revision >= %s
INFO  ---> first unlock
INFO  ---> second unlock
[watcher][worker]INFO  revision updated to "%s", unblocked "1" listeners
[task][%s]INFO  task succeeded (30s): slice closed
[task][%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver/my-export/slice.close/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z"
%A
INFO  exited
`, workerDeps.DebugLogger().AllMessages())
}
