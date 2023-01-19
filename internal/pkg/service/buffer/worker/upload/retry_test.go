package upload_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v4"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-client/pkg/storageapi/s3"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/upload"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type notRetryableError struct {
	error
}

// RetryableError is used by the AWS client.
func (v notRetryableError) RetryableError() bool {
	return false
}

func TestRetryBackoff(t *testing.T) {
	t.Parallel()

	b := upload.NewRetryBackoff()
	b.RandomizationFactor = 0

	clk := clock.NewMock()
	b.Clock = clk
	b.Reset()

	// Get all delays without sleep
	var delays []time.Duration
	for i := 0; i < 7; i++ {
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			assert.Fail(t, "received unexpected stop")
		}
		delays = append(delays, delay)
		clk.Add(delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		2 * time.Minute,
		8 * time.Minute,
		32 * time.Minute,
		128 * time.Minute,
		3 * time.Hour,
		3 * time.Hour,
		3 * time.Hour,
	}, delays)
}

func TestRetryAt(t *testing.T) {
	t.Parallel()

	b := upload.NewRetryBackoff()
	b.RandomizationFactor = 0
	now, _ := time.Parse(time.RFC3339, "2010-01-01T00:00:00Z")
	assert.Equal(t, "2010-01-01T00:02:00Z", upload.RetryAt(b, now, 1).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T00:10:00Z", upload.RetryAt(b, now, 2).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T00:42:00Z", upload.RetryAt(b, now, 3).Format(time.RFC3339)) // 2 + 8 + 32 = 42
	assert.Equal(t, "2010-01-01T02:50:00Z", upload.RetryAt(b, now, 4).Format(time.RFC3339)) // ...
	assert.Equal(t, "2010-01-01T05:50:00Z", upload.RetryAt(b, now, 5).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T08:50:00Z", upload.RetryAt(b, now, 6).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T11:50:00Z", upload.RetryAt(b, now, 7).Format(time.RFC3339))
}

func TestRetryFailedUploadsTask(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
	}

	// Create file
	file := &storageapi.File{
		Name:           "slice-upload-task-test",
		IsSliced:       true,
		Provider:       s3.Provider,
		Region:         "us‑east‑2",
		S3UploadParams: &s3.UploadParams{Key: "foo", Bucket: "bar"},
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
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err := upload.NewUploader(
		workerDeps,
		upload.WithUploadTransport(uploadTransport),
		upload.WithCloseSlices(true),
		upload.WithUploadSlices(true),
		upload.WithRetryFailedSlices(true),
	)
	assert.NoError(t, err)

	// Get slices
	slice, err := str.GetSlice(ctx, sliceKey)
	assert.NoError(t, err)

	// Switch slice to the closing state
	clk.Add(time.Minute)
	assert.NoError(t, str.SetSliceState(ctx, &slice, slicestate.Closing))
	assert.Eventually(t, func() bool {
		count, err := apiDeps1.Schema().Slices().Failed().Count().Do(ctx, client)
		assert.NoError(t, err)
		return count == 1
	}, 10*time.Second, 100*time.Millisecond)

	// 3 minutes later:
	// - triggers upload.FailedSlicesCheckInterval
	// - unblock the first backoff1 interval
	workerDeps.DebugLogger().Truncate()
	clk.Add(3 * time.Minute)

	// Shutdown
	time.Sleep(time.Second)
	apiDeps1.Process().Shutdown(errors.New("bye bye API 1"))
	apiDeps1.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()

	// Orchestrator logs
	wildcards.Assert(t, `
%A
[orchestrator][slice.retry.check]INFO  restart: periodical
[orchestrator][slice.retry.check]INFO  assigned "%s"
%A
`, strhelper.FilterLines(`^(\[orchestrator\]\[slice.retry.check\])`, workerDeps.DebugLogger().AllMessages()))

	// Retry check task
	wildcards.Assert(t, `
[task][slice.retry.check/%s]INFO  started task "00000123/my-receiver-1/my-export-1/slice.retry.check/%s"
[task][slice.retry.check/%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-1/my-export-1/slice.retry.check/%s"
[task][slice.retry.check/%s]INFO  task succeeded (%s): slice scheduled for retry
[task][slice.retry.check/%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-1/my-export-1/slice.retry.check/%s"
`, strhelper.FilterLines(`^(\[task\]\[slice.retry.check\/)`, workerDeps.DebugLogger().AllMessages()))

	// Retried upload
	wildcards.Assert(t, `
%A
[task][slice.upload/%s]INFO  started task %s
[task][slice.upload/%s]DEBUG  lock acquired %s
[task][slice.upload/%s]WARN  task failed (%s): slice upload failed: %s some network error, upload will be retried after "0001-01-01T00:%s" %s
[task][slice.upload/%s]DEBUG  lock released %s
%A
`, strhelper.FilterLines(`^(\[task\]\[slice.upload\/)|(\[orchestrator\]\[slice.upload\])`, workerDeps.DebugLogger().AllMessages()))

	// Check etcd state
	assertStateAfterRetry(t, client)
}

func assertStateAfterRetry(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVs(t, client, `
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
slice/failed/00000123/my-receiver-1/my-export-1/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "state": "failed",
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
    "recordsSize": 660,
    "bodySize": 90
  },
  "idRange": {
    "start": 1,
    "count": 5
  }
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/slice.close/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "type": "slice.close",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "my-worker",
  "lock": "slice.close/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "result": "slice closed",
  "duration": 0
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/slice.retry.check/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "type": "slice.retry.check",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "my-worker",
  "lock": "slice.retry.check/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "result": "slice scheduled for retry",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/slice.upload/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "type": "slice.upload",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "my-worker",
  "lock": "slice.upload/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "error": "slice upload failed: %s some network error, upload will be retried after \"%s\"",
  "duration": %d
}
>>>>>

<<<<<
task/00000123/my-receiver-1/my-export-1/slice.upload/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-1",
  "exportId": "my-export-1",
  "type": "slice.upload",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "my-worker",
  "lock": "slice.upload/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z",
  "error": "slice upload failed: %s some network error, upload will be retried after \"%s\"",
  "duration": %d
}
>>>>>
`)
}
