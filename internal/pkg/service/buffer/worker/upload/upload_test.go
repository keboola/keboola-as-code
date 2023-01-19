package upload_test

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/upload"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestSliceUploadTask(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	project := testproject.GetTestProjectForTest(t)
	opts := []dependencies.MockedOption{
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	// Create file
	file := &storageapi.File{
		Name:     "slice-upload-task-test",
		IsSliced: true,
	}
	if _, err := storageapi.CreateFileResourceRequest(file).Send(ctx, project.StorageAPIClient()); err != nil {
		assert.Fail(t, err.Error())
	}

	// Create receivers, exports and records
	apiDeps1 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node-1"))...)
	apiDeps2 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node-2"))...)
	str := apiDeps1.Store()
	emptySliceKey := createExport(t, "my-receiver-1", "my-export-1", ctx, clk, client, str, file)
	clk.Add(time.Minute)
	notEmptySliceKey := createExport(t, "my-receiver-2", "my-export-2", ctx, clk, client, str, file)
	clk.Add(time.Minute)
	createRecords(t, ctx, clk, apiDeps1, notEmptySliceKey.ReceiverKey, 1, 3)
	createRecords(t, ctx, clk, apiDeps2, notEmptySliceKey.ReceiverKey, 4, 4)
	assert.Eventually(t, func() bool {
		count, err := str.CountRecords(ctx, notEmptySliceKey)
		assert.NoError(t, err)
		return count == 7
	}, time.Second, 10*time.Millisecond)
	<-apiDeps1.StatsAPINode().Sync(ctx)
	<-apiDeps2.StatsAPINode().Sync(ctx)
	assertStateBeforeUpload(t, client)

	// Start worker node
	workerDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("my-worker"))...)
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err := upload.NewUploader(workerDeps, upload.WithCloseSlices(true), upload.WithUploadSlices(true))
	assert.NoError(t, err)

	// Get slices
	emptySlice, err := str.GetSlice(ctx, emptySliceKey)
	assert.NoError(t, err)
	notEmptySlice, err := str.GetSlice(ctx, notEmptySliceKey)
	assert.NoError(t, err)

	// Switch slices to the closing state
	clk.Add(time.Minute)
	assert.NoError(t, str.SetSliceState(ctx, &emptySlice, slicestate.Closing))
	assert.Eventually(t, func() bool {
		count, err := apiDeps1.Schema().Slices().Uploaded().Count().Do(ctx, client)
		assert.NoError(t, err)
		return count == 1
	}, 10*time.Second, 100*time.Millisecond)
	clk.Add(time.Minute)
	assert.NoError(t, str.SetSliceState(ctx, &notEmptySlice, slicestate.Closing))
	assert.Eventually(t, func() bool {
		count, err := apiDeps1.Schema().Slices().Uploaded().Count().Do(ctx, client)
		assert.NoError(t, err)
		return count == 2
	}, 10*time.Second, 100*time.Millisecond)

	// Shutdown
	time.Sleep(time.Second)
	apiDeps1.Process().Shutdown(errors.New("bye bye API 1"))
	apiDeps1.Process().WaitForShutdown()
	apiDeps2.Process().Shutdown(errors.New("bye bye API 2"))
	apiDeps2.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()

	// Check "close slice" logs
	wildcards.Assert(t, `
[orchestrator][slice.close]INFO  ready
[orchestrator][slice.close]INFO  assigned "00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z"
[task][slice.close/0001-01-01T00:03:08.000Z_%s]INFO  started task "00000123/my-receiver-1/my-export-1/slice.close/0001-01-01T00:03:08.000Z_%s"
[task][slice.close/0001-01-01T00:03:08.000Z_%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-1/my-export-1/slice.close/%s"
[task][slice.close/0001-01-01T00:03:08.000Z_%s]INFO  waiting until all API nodes switch to a revision >= %s
[task][slice.close/0001-01-01T00:03:08.000Z_%s]INFO  task succeeded (0s): slice closed
[task][slice.close/0001-01-01T00:03:08.000Z_%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-1/my-export-1/slice.close/%s"
[orchestrator][slice.close]INFO  assigned "00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z"
[task][slice.close/0001-01-01T00:04:08.000Z_%s]INFO  started task "00000123/my-receiver-2/my-export-2/slice.close/0001-01-01T00:04:08.000Z_%s"
[task][slice.close/0001-01-01T00:04:08.000Z_%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-2/my-export-2/slice.close/%s"
[task][slice.close/0001-01-01T00:04:08.000Z_%s]INFO  waiting until all API nodes switch to a revision >= %s
[task][slice.close/0001-01-01T00:04:08.000Z_%s]INFO  task succeeded (0s): slice closed
[task][slice.close/0001-01-01T00:04:08.000Z_%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-2/my-export-2/slice.close/%s"
[orchestrator][slice.close]INFO  stopped
`, strhelper.FilterLines(`^(\[task\]\[slice.close\/)|(\[orchestrator\]\[slice.close\])`, workerDeps.DebugLogger().AllMessages()))

	// Check "upload close" logs
	wildcards.Assert(t, `
[orchestrator][slice.upload]INFO  ready
[orchestrator][slice.upload]INFO  assigned "00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z"
[task][slice.upload/0001-01-01T00:03:08.000Z_%s]INFO  started task "00000123/my-receiver-1/my-export-1/slice.upload/0001-01-01T00:03:08.000Z_%s"
[task][slice.upload/0001-01-01T00:03:08.000Z_%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-1/my-export-1/%s"
[task][slice.upload/0001-01-01T00:03:08.000Z_%s]INFO  task succeeded (0s): skipped upload of the empty slice
[task][slice.upload/0001-01-01T00:03:08.000Z_%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-1/my-export-1/slice.upload/%s"
[orchestrator][slice.upload]INFO  assigned "00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z"
[task][slice.upload/0001-01-01T00:04:08.000Z_%s]INFO  started task "00000123/my-receiver-2/my-export-2/slice.upload/0001-01-01T00:04:08.000Z_%s"
[task][slice.upload/0001-01-01T00:04:08.000Z_%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-2/my-export-2/slice.upload/%s"
[task][slice.upload/0001-01-01T00:04:08.000Z_%s]INFO  task succeeded (0s): slice uploaded
[task][slice.upload/0001-01-01T00:04:08.000Z_%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-2/my-export-2/slice.upload/%s"
[orchestrator][slice.upload]INFO  stopped
`, strhelper.FilterLines(`^(\[task\]\[slice.upload\/)|(\[orchestrator\]\[slice.upload\])`, workerDeps.DebugLogger().AllMessages()))

	// Check etcd state
	assertStateAfterUpload(t, client)

	// Check content of the uploaded slice
	AssertUploadedSlice(t, ctx, file, notEmptySlice, strings.TrimLeft(`
1,0001-01-01T00:02:02.000Z,1.2.3.4,"{""key"":""value001""}","{""Content-Type"":""application/json""}","""---value001---"""
2,0001-01-01T00:02:03.000Z,1.2.3.4,"{""key"":""value002""}","{""Content-Type"":""application/json""}","""---value002---"""
3,0001-01-01T00:02:04.000Z,1.2.3.4,"{""key"":""value003""}","{""Content-Type"":""application/json""}","""---value003---"""
4,0001-01-01T00:02:05.000Z,1.2.3.4,"{""key"":""value004""}","{""Content-Type"":""application/json""}","""---value004---"""
5,0001-01-01T00:02:06.000Z,1.2.3.4,"{""key"":""value005""}","{""Content-Type"":""application/json""}","""---value005---"""
6,0001-01-01T00:02:07.000Z,1.2.3.4,"{""key"":""value006""}","{""Content-Type"":""application/json""}","""---value006---"""
7,0001-01-01T00:02:08.000Z,1.2.3.4,"{""key"":""value007""}","{""Content-Type"":""application/json""}","""---value007---"""
`, "\n"))
}

func assertStateBeforeUpload(t *testing.T, client *etcd.Client) {
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
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:05.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:05.000Z,1.2.3.4,"{""key"":""value004""}","{""Content-Type"":""application/json""}","""---value004---"""
>>>>>

<<<<<
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:06.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:06.000Z,1.2.3.4,"{""key"":""value005""}","{""Content-Type"":""application/json""}","""---value005---"""
>>>>>

<<<<<
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:07.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:07.000Z,1.2.3.4,"{""key"":""value006""}","{""Content-Type"":""application/json""}","""---value006---"""
>>>>>

<<<<<
record/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:02:08.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:02:08.000Z,1.2.3.4,"{""key"":""value007""}","{""Content-Type"":""application/json""}","""---value007---"""
>>>>>

<<<<<
runtime/api/node/watcher/cached/revision/api-node-1 (lease=%s)
-----
%A
>>>>>

<<<<<
runtime/api/node/watcher/cached/revision/api-node-2 (lease=%s)
-----
%A
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
slice/opened/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "state": "opened",
  "mapping": {
%A
  },
  "sliceNumber": 1
}
>>>>>

<<<<<
slice/opened/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-2",
  "exportId": "my-export-2",
  "fileId": "0001-01-01T00:01:01.000Z",
  "sliceId": "0001-01-01T00:01:01.000Z",
  "state": "opened",
  "mapping": {
%A
  },
  "sliceNumber": 1
}
>>>>>

<<<<<
stats/received/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z/api-node-1
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-2",
  "exportId": "my-export-2",
  "fileId": "0001-01-01T00:01:01.000Z",
  "sliceId": "0001-01-01T00:01:01.000Z",
  "lastRecordAt": "0001-01-01T00:02:04.000Z",
  "recordsCount": 3,
  "recordsSize": 396,
  "bodySize": 54
}
>>>>>

<<<<<
stats/received/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z/api-node-2
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-2",
  "exportId": "my-export-2",
  "fileId": "0001-01-01T00:01:01.000Z",
  "sliceId": "0001-01-01T00:01:01.000Z",
  "lastRecordAt": "0001-01-01T00:02:08.000Z",
  "recordsCount": 4,
  "recordsSize": 528,
  "bodySize": 72
}
>>>>>
`)
}

func assertStateAfterUpload(t *testing.T, client *etcd.Client) {
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
runtime/last/record/id/00000123/my-receiver-2/my-export-2
-----
7
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
slice/uploaded/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "state": "uploaded",
  "isEmpty": true,
  "mapping": {
%A
  },
  "sliceNumber": 1,
  "closingAt": "0001-01-01T00:03:08.000Z",
  "uploadingAt": "0001-01-01T00:03:08.000Z",
  "uploadedAt": "0001-01-01T00:03:08.000Z"
}
>>>>>

<<<<<
slice/uploaded/00000123/my-receiver-2/my-export-2/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-2",
  "exportId": "my-export-2",
  "fileId": "0001-01-01T00:01:01.000Z",
  "sliceId": "0001-01-01T00:01:01.000Z",
  "state": "uploaded",
  "mapping": {
%A
  },
  "sliceNumber": 1,
  "closingAt": "0001-01-01T00:04:08.000Z",
  "uploadingAt": "0001-01-01T00:04:08.000Z",
  "uploadedAt": "0001-01-01T00:04:08.000Z",
  "statistics": {
    "lastRecordAt": "0001-01-01T00:02:08.000Z",
    "recordsCount": 7,
    "recordsSize": 924,
    "bodySize": 126,
    "fileSize": 861,
    "fileGZipSize": 195
  },
  "idRange": {
    "start": 1,
    "count": 7
  }
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/slice.close/0001-01-01T00:03:08.000Z_%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "type": "slice.close",
  "createdAt": "0001-01-01T00:03:08.000Z",
  "randomId": "%s",
  "finishedAt": "0001-01-01T00:03:08.000Z",
  "workerNode": "my-worker",
  "lock": "slice.close/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "result": "slice closed",
  "duration": 0
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/slice.upload/0001-01-01T00:03:08.000Z_%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "type": "slice.upload",
  "createdAt": "0001-01-01T00:03:08.000Z",
  "randomId": "%s",
  "finishedAt": "0001-01-01T00:03:08.000Z",
  "workerNode": "my-worker",
  "lock": "slice.upload/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "result": "skipped upload of the empty slice",
  "duration": 0
}
>>>>>

<<<<<
task/00000123/my-receiver-2/my-export-2/slice.close/0001-01-01T00:04:08.000Z_%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-2",
  "exportId": "my-export-2",
  "type": "slice.close",
  "createdAt": "0001-01-01T00:04:08.000Z",
  "randomId": "%s",
  "finishedAt": "0001-01-01T00:04:08.000Z",
  "workerNode": "my-worker",
  "lock": "slice.close/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z",
  "result": "slice closed",
  "duration": 0
}
>>>>>

<<<<<
task/00000123/my-receiver-2/my-export-2/slice.upload/0001-01-01T00:04:08.000Z_%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-2",
  "exportId": "my-export-2",
  "type": "slice.upload",
  "createdAt": "0001-01-01T00:04:08.000Z",
  "randomId": "%s",
  "finishedAt": "0001-01-01T00:04:08.000Z",
  "workerNode": "my-worker",
  "lock": "slice.upload/0001-01-01T00:01:01.000Z/0001-01-01T00:01:01.000Z",
  "result": "slice uploaded",
  "duration": 0
}
>>>>>
`)
}

func AssertUploadedSlice(t *testing.T, ctx context.Context, file *storageapi.File, slice model.Slice, expected string) {
	t.Helper()

	// Get file content
	sliceURL := strings.ReplaceAll(file.Url, file.Name+"manifest", file.Name+slice.Filename())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sliceURL, nil)
	assert.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Read file content
	gz, err := gzip.NewReader(resp.Body)
	assert.NoError(t, err)
	data, err := io.ReadAll(gz)
	_ = resp.Body.Close()
	_ = gz.Close()
	assert.NoError(t, err)
	assert.NoError(t, gz.Close())

	// Compare
	assert.Equal(t, expected, string(data))
}
