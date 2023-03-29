package service_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	workerConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type notRetryableError struct {
	error
}

// RetryableError disables retries in S3 AWS client.
func (v notRetryableError) RetryableError() bool {
	return false
}

// TestRetryFailedUploadsTask - the worker switches the "failed" slice to the "uploading" state,
// after the slice.RetryAfter interval.
func TestRetryFailedUploadsTask(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
	}

	// Create file
	file := &keboola.FileUploadCredentials{
		File: keboola.File{
			Name:     "slice-upload-task-test",
			IsSliced: true,
			Provider: s3.Provider,
			Region:   "us‑east‑2",
		},
		S3UploadParams: &s3.UploadParams{Path: s3.Path{Key: "foo", Bucket: "bar"}},
	}

	// Create receivers, exports and records
	apiDeps1 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node-1"))...)
	str := apiDeps1.Store()
	sliceKey := createExport(t, "my-receiver-1", "my-export-1", ctx, clk, client, str, file)
	createRecords(t, ctx, clk, apiDeps1, sliceKey.ReceiverKey, 1, 5)
	clk.Add(time.Minute)

	// Requests to the file storage will fail
	uploadTransport := httpmock.NewMockTransport()
	uploadTransport.RegisterNoResponder(func(request *http.Request) (*http.Response, error) {
		return nil, notRetryableError{error: errors.New("some network error")}
	})

	// Start worker node
	workerDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("my-worker"))...)
	workerDeps.SetWorkerConfigOps(
		workerConfig.WithUploadTransport(uploadTransport),
		workerConfig.WithConditionsCheck(false),
		workerConfig.WithCleanup(false),
		workerConfig.WithCloseSlices(true),
		workerConfig.WithUploadSlices(true),
		workerConfig.WithRetryFailedSlices(true),
		workerConfig.WithCloseFiles(false),
		workerConfig.WithImportFiles(false),
		workerConfig.WithRetryFailedFiles(false),
	)
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err := service.New(workerDeps)
	assert.NoError(t, err)

	// Get slices
	slice, err := str.GetSlice(ctx, sliceKey)
	assert.NoError(t, err)

	// Switch slice to the closing state
	clk.Add(time.Minute)
	assert.NoError(t, str.SetSliceState(ctx, &slice, slicestate.Closing))
	clk.Add(time.Minute) // sync revision from API nodes
	assert.Eventually(t, func() bool {
		count, err := apiDeps1.Schema().Slices().Failed().Count().Do(ctx, client)
		assert.NoError(t, err)
		return count == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Wait for failed upload
	assert.Eventually(t, func() bool {
		return strings.Count(workerDeps.DebugLogger().WarnMessages(), "WARN  task failed") == 1
	}, 10*time.Second, 100*time.Millisecond)
	workerDeps.DebugLogger().Truncate()

	// 3 minutes later:
	// - triggers service.FailedSlicesCheckInterval
	// - unblock the first backoff1 interval
	clk.Add(3 * time.Minute)

	// Wait for retry
	assert.Eventually(t, func() bool {
		return strings.Count(workerDeps.DebugLogger().WarnMessages(), "WARN  task failed") == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Shutdown
	apiDeps1.Process().Shutdown(errors.New("bye bye API 1"))
	apiDeps1.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()

	// Orchestrator logs
	assert.Contains(t, workerDeps.DebugLogger().AllMessages(), "[orchestrator][slice.retry.check]INFO  assigned")
	wildcards.Assert(t, `
[orchestrator][slice.retry.check]INFO  assigned "00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check"
[orchestrator][slice.retry.check]INFO  stopped
`, strhelper.FilterLines(`^(\[orchestrator\]\[slice.retry.check\])`, workerDeps.DebugLogger().InfoMessages()))

	// Retry check task
	wildcards.Assert(t, `
[task][%s]INFO  started task
[task][%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check"
[task][%s]INFO  task succeeded (%s): slice scheduled for retry
[task][%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check"
`, strhelper.FilterLines(`^(\[task\]\[.+\/slice.retry.check\/)`, workerDeps.DebugLogger().AllMessages()))

	// Retried upload
	wildcards.Assert(t, `
[task][%s]WARN  task failed (%s): slice upload failed: %s some network error, upload will be retried after "0001-01-01T00:%s" %s
`, strhelper.FilterLines(`^\[task\]\[.+\/slice.upload\/`, workerDeps.DebugLogger().WarnMessages()))

	// Check etcd state
	assertStateAfterRetry(t, client)
}

func assertStateAfterRetry(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVsString(t, client, `
<<<<<
config/export/00000123/my-receiver-1/my-export-1
-----
%A
>>>>>

<<<<<
config/mapping/revision/00000123/my-receiver-1/my-export-1/00000001
-----
%A
>>>>>

<<<<<
config/receiver/00000123/my-receiver-1
-----
%A
>>>>>

<<<<<
file/opened/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z
-----
%A
>>>>>

<<<<<
record/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:02.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:02.000Z,1.2.3.4,"{""key"":""value001""}","{""Content-Type"":""application/json""}","""---value001---"""
>>>>>

<<<<<
record/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:03.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:03.000Z,1.2.3.4,"{""key"":""value002""}","{""Content-Type"":""application/json""}","""---value002---"""
>>>>>

<<<<<
record/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:04.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:04.000Z,1.2.3.4,"{""key"":""value003""}","{""Content-Type"":""application/json""}","""---value003---"""
>>>>>

<<<<<
record/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:05.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:05.000Z,1.2.3.4,"{""key"":""value004""}","{""Content-Type"":""application/json""}","""---value004---"""
>>>>>

<<<<<
record/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:06.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:06.000Z,1.2.3.4,"{""key"":""value005""}","{""Content-Type"":""application/json""}","""---value005---"""
>>>>>

<<<<<
runtime/last/record/id/00000123/my-receiver-1/my-export-1
-----
5
>>>>>

<<<<<
secret/export/token/00000123/my-receiver-1/my-export-1
-----
%A
>>>>>

<<<<<
slice/active/closed/failed/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "state": "active/closed/failed",
  "mapping": {
%A
  },
  "sliceNumber": 1,
  "closingAt": "0001-01-01T00:02:06.000Z",
  "uploadingAt": "%s",
  "failedAt": "%s",
  "retryAttempt": 2,
  "retryAfter": "0001-01-01T00:%sZ",
  "statistics": {
    "lastRecordAt": "0001-01-01T00:00:06.000Z",
    "recordsCount": 5,
    "recordsSize": "660B",
    "bodySize": "90B"
  },
  "idRange": {
    "start": 1,
    "count": 5
  }
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.close/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.close/%s",
  "type": "slice.close",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.close",
  "result": "slice closed",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check/%s",
  "type": "slice.retry.check",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check",
  "result": "slice scheduled for retry",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s",
  "type": "slice.upload",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload",
  "error": "slice upload failed: %s some network error, upload will be retried after \"%s\"",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s",
  "type": "slice.upload",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload",
  "error": "slice upload failed: %s some network error, upload will be retried after \"%s\"",
  "duration": %d
}
>>>>>
`)
}
