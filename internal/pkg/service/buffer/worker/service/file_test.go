package service_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	service2 "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestUploadAndImportE2E(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	_ = etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	project := testproject.GetTestProjectForTest(t)
	opts := []dependencies.MockedOption{
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	// Start API node
	apiDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node"))...)
	apiDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	api := service2.New(apiDeps)

	// Create receiver and export
	receiver, secret, export := createReceiverAndExportViaAPI(t, apiDeps, api)

	// Start worker node
	workerDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("worker-node"))...)
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	logger := workerDeps.DebugLogger()
	_, err := service.New(
		workerDeps,
		service.WithCheckConditionsInterval(500*time.Millisecond),
		service.WithUploadConditions(model.Conditions{Count: 5, Size: datasize.MB, Time: time.Hour}),
	)
	assert.NoError(t, err)

	// Create 6 records - trigger the slice upload (>=5)
	for i := 1; i <= 6; i++ {
		assert.NoError(t, api.Import(apiDeps, &buffer.ImportPayload{
			ProjectID:  buffer.ProjectID(project.ID()),
			ReceiverID: receiver.ID,
			Secret:     secret,
		}, io.NopCloser(strings.NewReader(fmt.Sprintf(`payload%03d`, i)))))
	}

	// Wait for upload
	assert.Eventually(t, func() bool {
		logs := logger.AllMessages()
		conditionsOk := wildcards.Compare("%A[service][conditions]INFO  closing slice \"%s\": count threshold met, received: 6 rows, threshold: 5 rows%A", strhelper.FilterLines(`\[service\]\[conditions\]`, logs)) == nil
		sliceCloseOk := wildcards.Compare("%A[task][slice.close/%s]INFO  task succeeded (%s): slice closed%A", strhelper.FilterLines(`\[task\]\[slice.close`, logs)) == nil
		sliceUploadOk := wildcards.Compare("%A[task][slice.upload/%s]INFO  task succeeded (%s): slice uploaded%A", strhelper.FilterLines(`\[task\]\[slice.upload`, logs)) == nil
		return conditionsOk && sliceCloseOk && sliceUploadOk
	}, 60*time.Second, 100*time.Millisecond, logger.AllMessages())
	logger.Truncate()

	// Create next 4 records - trigger the file import (>=10)
	for i := 7; i <= 10; i++ {
		assert.NoError(t, api.Import(apiDeps, &buffer.ImportPayload{
			ProjectID:  buffer.ProjectID(project.ID()),
			ReceiverID: receiver.ID,
			Secret:     secret,
		}, io.NopCloser(strings.NewReader(fmt.Sprintf(`payload%03d`, i)))))
	}

	// Wait for import
	assert.Eventually(t, func() bool {
		logs := logger.AllMessages()
		conditionsOk := wildcards.Compare("%A[service][conditions]INFO  closing file \"%s\": count threshold met, received: 10 rows, threshold: 10 rows%A", strhelper.FilterLines(`\[service\]\[conditions\]`, logs)) == nil
		sliceCloseOk := wildcards.Compare("%A[task][slice.close/%s]INFO  task succeeded (%s): slice closed%A", strhelper.FilterLines(`\[task\]\[slice.close`, logs)) == nil
		sliceUploadOk := wildcards.Compare("%A[task][slice.upload/%s]INFO  task succeeded (%s): slice uploaded%A", strhelper.FilterLines(`\[task\]\[slice.upload`, logs)) == nil
		fileCloseWaitOk := wildcards.Compare("%A[task][file.close/%s]INFO  waiting for \"1\" slices to be uploaded%A", strhelper.FilterLines(`\[task\]\[file.close`, logs)) == nil
		fileCloseOk := wildcards.Compare("%A[task][file.close/%s]INFO  task succeeded (%s): file closed%A", strhelper.FilterLines(`\[task\]\[file.close`, logs)) == nil
		fileImportOk := wildcards.Compare("%A[task][file.import/%s]INFO  task succeeded (%s): file imported%A", strhelper.FilterLines(`\[task\]\[file.import`, logs)) == nil
		return conditionsOk && sliceCloseOk && sliceUploadOk && fileCloseWaitOk && fileCloseOk && fileImportOk
	}, 60*time.Second, 100*time.Millisecond, logger.AllMessages())
	logger.Truncate()

	// Check the target table
	table, err := storageapi.
		GetTableRequest(storageapi.MustParseTableID(export.Mapping.TableID)).
		Send(ctx, project.StorageAPIClient())
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), table.RowsCount)

	// Check etcd state
	assertStateAfterImport(t, client)

	// Shutdown
	apiDeps.Process().Shutdown(errors.New("bye bye API"))
	apiDeps.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()
}

func assertStateAfterImport(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVs(t, client, `
<<<<<
config/export/%s/my-receiver/my-export
-----
%A
>>>>>

<<<<<
config/mapping/revision/%s/my-receiver/my-export/00000001
-----
%A
>>>>>

<<<<<
config/receiver/%s/my-receiver
-----
%A
>>>>>

<<<<<
file/imported/%s/my-receiver/my-export/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "%A",
  "state": "imported",
  "mapping": {
%A
  },
  "storageResource": {
%A
  },
  "closingAt": "%s",
  "importingAt": "%s",
  "importedAt": "%s"
}
>>>>>

<<<<<
file/opened/%s/my-receiver/my-export/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "%s",
  "state": "opened",
  "mapping": {
%A
  },
  "storageResource": {
%A
  }
}
>>>>>

<<<<<
runtime/api/node/watcher/cached/revision/api-node (lease=%d)
-----
%d
>>>>>

<<<<<
runtime/last/record/id/%s/my-receiver/my-export
-----
10
>>>>>

<<<<<
runtime/worker/node/active/id/worker-node (lease=%d)
-----
worker-node
>>>>>

<<<<<
secret/export/token/%s/my-receiver/my-export
-----
%A
>>>>>

<<<<<
slice/active/opened/writing/%s/my-receiver/my-export/%s/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "%s",
  "sliceId": "%s",
  "state": "active/opened/writing",
  "mapping": {
%A
  },
  "storageResource": {
%A
  },
  "sliceNumber": 1
}
>>>>>

<<<<<
slice/archived/successful/imported/%s/my-receiver/my-export/%s/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "%s",
  "sliceId": "%s",
  "state": "archived/successful/imported",
  "mapping": {
%A
  },
  "storageResource": {
%A
  },
  "sliceNumber": 1,
  "closingAt": "%s",
  "uploadingAt": "%s",
  "uploadedAt": "%s",
  "importedAt": "%s",
  "statistics": {
    "lastRecordAt": "%s",
    "recordsCount": 6,
    "recordsSize": "282B",
    "bodySize": "60B",
    "fileSize": "228B",
    "fileGZipSize": "%s"
  },
  "idRange": {
    "start": 1,
    "count": 6
  }
}
>>>>>

<<<<<
slice/archived/successful/imported/%s/my-receiver/my-export/%s/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "%s",
  "sliceId": "%s",
  "state": "archived/successful/imported",
  "mapping": {
%A
  },
  "storageResource": {
%A
  },
  "sliceNumber": 2,
  "closingAt": "%s",
  "uploadingAt": "%s",
  "uploadedAt": "%s",
  "importedAt": "%s",
  "statistics": {
    "lastRecordAt": "%s",
    "recordsCount": 4,
    "recordsSize": "188B",
    "bodySize": "40B",
    "fileSize": "153B",
    "fileGZipSize": "%s"
  },
  "idRange": {
    "start": 7,
    "count": 4
  }
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/file.close/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "file.close",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "file.close/%s",
  "result": "file closed",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/file.import/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "file.import",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "file.import/%s",
  "result": "file imported",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/file.swap/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "file.swap",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "file.swap/%s",
  "result": "new file created, the old is closing",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/slice.close/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "slice.close",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "slice.close/%s",
  "result": "slice closed",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/slice.close/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "slice.close",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "slice.close/%s",
  "result": "slice closed",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/slice.swap/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "slice.swap",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "slice.swap/%s",
  "result": "new slice created, the old is closing",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/slice.upload/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "slice.upload",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "slice.upload/%s",
  "result": "slice uploaded",
  "duration": %d
}
>>>>>

<<<<<
task/%s/my-receiver/my-export/slice.upload/%s
-----
{
  "projectId": %d,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "type": "slice.upload",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node",
  "lock": "slice.upload/%s",
  "result": "slice uploaded",
  "duration": %d
}
>>>>>
`)
}
