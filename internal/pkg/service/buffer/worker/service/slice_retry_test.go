package service_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	config "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
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

	etcdCredentials := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCredentials)

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	opts := []dependencies.MockedOption{
		dependencies.WithEnabledOrchestrator(),
		dependencies.WithClock(clk),
		dependencies.WithEtcdCredentials(etcdCredentials),
	}

	// Create file
	file := &keboola.FileUploadCredentials{
		File: keboola.File{
			Name:     "slice-upload-task-test",
			IsSliced: true,
			Provider: s3.Provider,
			Region:   "us‑east‑2",
		},
		S3UploadParams: &s3.UploadParams{
			Path: s3.Path{Key: "foo", Bucket: "bar"},
			Credentials: s3.Credentials{
				AccessKeyID:     "foo",
				SecretAccessKey: "bar",
			},
		},
	}

	// Create receivers, exports and records
	apiScp, _ := bufferDependencies.NewMockedAPIScope(t, config.NewAPIConfig(), append(opts, dependencies.WithUniqueID("api-node-1"))...)
	str := apiScp.Store()
	sliceKey := createExport(t, "my-receiver-1", "my-export-1", ctx, clk, client, str, file)
	createRecords(t, ctx, clk, apiScp, sliceKey.ReceiverKey, 1, 5)
	clk.Add(time.Minute)

	// Enable only WithCloseSlices, WithUploadSlices
	workerConfig := config.NewWorkerConfig().Apply(
		config.WithConditionsCheck(false),
		config.WithCleanup(false),
		config.WithCloseSlices(true),
		config.WithUploadSlices(true),
		config.WithRetryFailedSlices(true),
		config.WithCloseFiles(false),
		config.WithImportFiles(false),
		config.WithRetryFailedFiles(false),
	)

	// Requests to the file storage will fail
	uploadTransport := httpmock.NewMockTransport()
	uploadTransport.RegisterNoResponder(func(request *http.Request) (*http.Response, error) {
		return nil, notRetryableError{error: errors.New("some network error")}
	})
	workerConfig.UploadTransport = uploadTransport

	// Start worker node
	workerScp, workerMock := bufferDependencies.NewMockedWorkerScope(t, workerConfig, append(opts, dependencies.WithUniqueID("my-worker"))...)
	_, err := service.New(workerScp)
	assert.NoError(t, err)

	// Mock events send request
	workerMock.MockedHTTPTransport().RegisterResponder(
		http.MethodPost,
		"https://mocked.transport.http/v2/storage/events",
		httpmock.NewJsonResponderOrPanic(http.StatusOK, &keboola.Event{ID: "123"}),
	)

	// Get slices
	slice, err := str.GetSlice(ctx, sliceKey)
	assert.NoError(t, err)

	// Switch slice to the closing state
	clk.Add(time.Minute)
	assert.NoError(t, str.SetSliceState(ctx, &slice, slicestate.Closing))
	clk.Add(time.Minute) // sync revision from API nodes
	assert.Eventually(t, func() bool {
		count, err := apiScp.Schema().Slices().Failed().Count().Do(ctx, client)
		assert.NoError(t, err)
		return count == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Wait for failed upload
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"warn","message":"task failed %A"}`, workerMock.DebugLogger().WarnMessages()) == nil
	}, 30*time.Second, 100*time.Millisecond)
	workerMock.DebugLogger().Truncate()

	// 3 minutes later:
	// - triggers service.FailedSlicesCheckInterval
	// - unblock the first backoff interval
	clk.Add(3 * time.Minute)

	// Wait for retry
	assert.Eventually(t, func() bool {
		return log.CompareJSONMessages(`{"level":"warn","message":"task failed %A"}`, workerMock.DebugLogger().WarnMessages()) == nil
	}, 30*time.Second, 100*time.Millisecond)

	// Shutdown
	apiScp.Process().Shutdown(ctx, errors.New("bye bye API 1"))
	apiScp.Process().WaitForShutdown()
	workerScp.Process().Shutdown(ctx, errors.New("bye bye Worker"))
	workerScp.Process().WaitForShutdown()

	// Orchestrator logs
	log.AssertJSONMessages(t, `
{"level":"info","message":"assigned %s","component":"orchestrator","prefix":"[slice.retry.check]"}
`, workerMock.DebugLogger().AllMessages())
	log.AssertJSONMessages(t, `
{"level":"info","message":"assigned \"123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check\"","component":"orchestrator","prefix":"[slice.retry.check]"}
{"level":"info","message":"stopped","component":"orchestrator","prefix":"[slice.retry.check]"}
`, workerMock.DebugLogger().InfoMessages())

	// Retry check task
	log.AssertJSONMessages(t, `
{"level":"info","message":"started task","component":"task","task":"%s/slice.retry.check/%s"}
{"level":"debug","message":"lock acquired \"runtime/lock/task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check\"","component":"task","task":"%s/slice.retry.check/%s"}
{"level":"info","message":"task succeeded (%s): slice scheduled for retry","component":"task","task":"%s/slice.retry.check/%s"}
{"level":"debug","message":"lock released \"runtime/lock/task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check\"","component":"task","task":"%s/slice.retry.check/%s"}
`, workerMock.DebugLogger().AllMessages())

	// Retried upload
	log.AssertJSONMessages(t, `
{"level":"warn","message":"task failed (%s): slice upload failed: %A some network error, upload will be retried after \"0001-01-01T00:%s\" %A","component":"task","task":"%s/slice.upload/%s"}
`, workerMock.DebugLogger().WarnMessages())

	// Check etcd state
	assertStateAfterRetry(t, client)
}

func assertStateAfterRetry(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVsString(t, client, `
<<<<<
config/export/123/my-receiver-1/my-export-1
-----
%A
>>>>>

<<<<<
config/mapping/revision/123/my-receiver-1/my-export-1/00000001
-----
%A
>>>>>

<<<<<
config/receiver/123/my-receiver-1
-----
%A
>>>>>

<<<<<
file/opened/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z
-----
%A
>>>>>

<<<<<
record/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:02.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:02.000Z,1.2.3.4,"{""key"":""value001""}","{""Content-Type"":""application/json""}","""---value001---"""
>>>>>

<<<<<
record/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:03.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:03.000Z,1.2.3.4,"{""key"":""value002""}","{""Content-Type"":""application/json""}","""---value002---"""
>>>>>

<<<<<
record/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:04.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:04.000Z,1.2.3.4,"{""key"":""value003""}","{""Content-Type"":""application/json""}","""---value003---"""
>>>>>

<<<<<
record/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:05.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:05.000Z,1.2.3.4,"{""key"":""value004""}","{""Content-Type"":""application/json""}","""---value004---"""
>>>>>

<<<<<
record/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:06.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:06.000Z,1.2.3.4,"{""key"":""value005""}","{""Content-Type"":""application/json""}","""---value005---"""
>>>>>

<<<<<
runtime/last/record/id/123/my-receiver-1/my-export-1
-----
5
>>>>>

<<<<<
secret/export/token/123/my-receiver-1/my-export-1
-----
%A
>>>>>

<<<<<
slice/active/closed/failed/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
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
  "idRange": {
    "start": 1,
    "count": 5
  }
}
>>>>>

<<<<<
stats/buffered/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/api-node-1
-----
{
  "firstRecordAt": "0001-01-01T00:00:02.000Z",
  "lastRecordAt": "0001-01-01T00:00:06.000Z",
  "recordsCount": 5,
  "recordsSize": "660B",
  "bodySize": "90B"
}
>>>>>


<<<<<
task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.close/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.close/%s",
  "type": "slice.close",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.close",
  "result": "slice closed",
  "duration": %d
}
>>>>>

<<<<<
task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check/%s",
  "type": "slice.retry.check",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.retry.check",
  "result": "slice scheduled for retry",
  "duration": %d
}
>>>>>

<<<<<
task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s",
  "type": "slice.upload",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload",
  "error": "slice upload failed: %s some network error, upload will be retried after \"%s\"",
  "duration": %d
}
>>>>>

<<<<<
task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s
-----
{
  "projectId": 123,
  "taskId": "my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload/%s",
  "type": "slice.upload",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "my-worker",
  "lock": "runtime/lock/task/123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/slice.upload",
  "error": "slice upload failed: %s some network error, upload will be retried after \"%s\"",
  "duration": %d
}
>>>>>
`)
}
