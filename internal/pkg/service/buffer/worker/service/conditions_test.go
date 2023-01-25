package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestConditionsChecker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create nodes
	clk := clock.NewMock()
	clk.Set(time.Time{})
	clk.Add(time.Second)
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	project := testproject.GetTestProjectForTest(t)
	opts := []dependencies.MockedOption{
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	// Create receivers, exports and records
	apiDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("api-node-1"))...)
	apiStats := apiDeps.StatsCollector()
	str := apiDeps.Store()
	importConditions1 := model.Conditions{Count: 100, Size: 200 * datasize.KB, Time: time.Hour}
	file1 := &storageapi.File{Name: "file 1", IsSliced: true}
	clk.Add(time.Second)
	importConditions2 := model.Conditions{Count: 1000, Size: 20 * datasize.MB, Time: 5 * time.Hour}
	file2 := &storageapi.File{Name: "file 1", IsSliced: true}
	if _, err := storageapi.CreateFileResourceRequest(file1).Send(ctx, project.StorageAPIClient()); err != nil {
		assert.Fail(t, err.Error())
	}
	if _, err := storageapi.CreateFileResourceRequest(file2).Send(ctx, project.StorageAPIClient()); err != nil {
		assert.Fail(t, err.Error())
	}
	sliceKey1 := createExport2(t, "my-receiver-A", "my-export-1", ctx, clk, client, str, file1, importConditions1, project.StorageAPIToken().Token)
	sliceKey2 := createExport2(t, "my-receiver-B", "my-export-2", ctx, clk, client, str, file2, importConditions2, project.StorageAPIToken().Token)

	// Create nodes
	workerDeps1 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("worker-node-1"))...)
	workerDeps2 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("worker-node-2"))...)
	workerDeps1.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	workerDeps2.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	serviceOps := []service.Option{
		service.WithCheckConditions(true),
		service.WithCloseSlices(false),
		service.WithUploadSlices(false),
		service.WithRetryFailedSlices(false),
		service.WithCloseFiles(false),
		service.WithImportFiles(false),
		service.WithRetryFailedFiles(false),
	}
	_, err := service.New(workerDeps1, serviceOps...)
	assert.NoError(t, err)
	_, err = service.New(workerDeps2, serviceOps...)
	assert.NoError(t, err)

	time.Sleep(time.Second)
	clk.Add(service.DefaultCheckConditionsInterval)
	apiStats.Notify(sliceKey1, 100*datasize.KB, 300*datasize.KB)
	<-apiStats.Sync(ctx)
	time.Sleep(time.Second)
	clk.Add(service.DefaultCheckConditionsInterval)
	apiStats.Notify(sliceKey1, 150*datasize.KB, 300*datasize.KB)
	apiStats.Notify(sliceKey2, 10*datasize.KB, 10*datasize.KB)
	<-apiStats.Sync(ctx)
	time.Sleep(time.Second)
	clk.Add(service.DefaultCheckConditionsInterval)
	time.Sleep(time.Second)
	clk.Add(service.DefaultCheckConditionsInterval)

	// Shutdown
	time.Sleep(2 * time.Second)
	apiDeps.Process().Shutdown(errors.New("bye bye API"))
	apiDeps.Process().WaitForShutdown()
	workerDeps1.Process().Shutdown(errors.New("bye bye Worker 1"))
	workerDeps1.Process().WaitForShutdown()
	workerDeps2.Process().Shutdown(errors.New("bye bye Worker 2"))
	workerDeps2.Process().WaitForShutdown()

	// Check conditions checker logs
	wildcards.Assert(t, `
%A
[service][conditions]INFO  checked "1" opened slices | %s
[service][conditions]INFO  checked "1" opened slices | %s
[service][conditions]INFO  checked "1" opened slices | %s
[service][conditions]INFO  closing slice "00000123/my-receiver-B/my-export-2/0001-01-01T00:00:02.000Z/0001-01-01T00:00:02.000Z": time threshold met, opened at: 0001-01-01T00:00:02.000Z, passed: 1m30s threshold: 1m0s
%A
`, strhelper.FilterLines(`^(\[service\]\[conditions\])`, workerDeps1.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
%A
[service][conditions]INFO  checked "1" opened slices | %s
[service][conditions]INFO  checked "1" opened slices | %s
[service][conditions]INFO  closing slice "00000123/my-receiver-A/my-export-1/0001-01-01T00:00:02.000Z/0001-01-01T00:00:02.000Z": time threshold met, opened at: 0001-01-01T00:00:02.000Z, passed: 1m0s threshold: 1m0s
[service][conditions]INFO  checked "1" opened slices | %s
[service][conditions]INFO  closing file "00000123/my-receiver-A/my-export-1/0001-01-01T00:00:02.000Z": size threshold met, received: 250KB, threshold: 200KB
%A
`, strhelper.FilterLines(`^(\[service\]\[conditions\])`, workerDeps2.DebugLogger().AllMessages()))

	// Check conditions checker logs
	wildcards.Assert(t, `
%A
[task][slice.swap/%s]INFO  started task "00000123/my-receiver-B/my-export-2/slice.swap/%s"
[task][slice.swap/%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-B/my-export-2/slice.swap/%s"
[task][slice.swap/%s]INFO  task succeeded (%s): new slice created, the old is closing
[task][slice.swap/%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-B/my-export-2/slice.swap/%s"
%A
`, strhelper.FilterLines(`^(\[task\])`, workerDeps1.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
%A
[task][slice.swap/%s]INFO  started task "00000123/my-receiver-A/my-export-1/slice.swap/%s"
[task][slice.swap/%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-A/my-export-1/slice.swap/%s"
[task][slice.swap/%s]INFO  task succeeded (%s): new slice created, the old is closing
[task][slice.swap/%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-A/my-export-1/slice.swap/%s"
[task][file.swap/%s]INFO  started task "00000123/my-receiver-A/my-export-1/file.swap/%s"
[task][file.swap/%s]DEBUG  lock acquired "runtime/lock/task/00000123/my-receiver-A/my-export-1/file.swap/%s"
[task][file.swap/%s]INFO  task succeeded (%s): new file created, the old is closing
[task][file.swap/%s]DEBUG  lock released "runtime/lock/task/00000123/my-receiver-A/my-export-1/file.swap/%s"
%A
`, strhelper.FilterLines(`^(\[task\])`, workerDeps2.DebugLogger().AllMessages()))

	// Check etcd state
	assertStateAfterTest(t, client)

	// After deleting the receivers, the database should remain empty
	assert.NoError(t, str.DeleteReceiver(ctx, sliceKey1.ReceiverKey))
	assert.NoError(t, str.DeleteReceiver(ctx, sliceKey2.ReceiverKey))
	etcdhelper.AssertKVs(t, client, "")
}

func assertStateAfterTest(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVs(t, client, `
<<<<<
config/export/00000123/my-receiver-A/my-export-1
-----
%A
>>>>>

<<<<<
config/export/00000123/my-receiver-B/my-export-2
-----
%A
>>>>>

<<<<<
config/mapping/revision/00000123/my-receiver-A/my-export-1/00000001
-----
%A
>>>>>

<<<<<
config/mapping/revision/00000123/my-receiver-B/my-export-2/00000001
-----
%A
>>>>>

<<<<<
config/receiver/00000123/my-receiver-A
-----
%A
>>>>>

<<<<<
config/receiver/00000123/my-receiver-B
-----
%A
>>>>>

<<<<<
file/closing/00000123/my-receiver-A/my-export-1/0001-01-01T00:00:02.000Z
-----
%A
>>>>>

<<<<<
file/opened/00000123/my-receiver-A/my-export-1/0001-01-01T%s
-----
%A
>>>>>

<<<<<
file/opened/00000123/my-receiver-B/my-export-2/0001-01-01T00:00:02.000Z
-----
%A
>>>>>

<<<<<
secret/export/token/00000123/my-receiver-A/my-export-1
-----
%A
>>>>>

<<<<<
secret/export/token/00000123/my-receiver-B/my-export-2
-----
%A
>>>>>

<<<<<
slice/active/opened/closing/00000123/my-receiver-A/my-export-1/0001-01-01T00:00:02.000Z/0001-01-01T00:00:02.000Z
-----
%A
>>>>>

<<<<<
slice/active/opened/closing/00000123/my-receiver-A/my-export-1/0001-01-01T00:00:02.000Z/0001-01-01T00:01:02.000Z
-----
%A
>>>>>

<<<<<
slice/active/opened/closing/00000123/my-receiver-B/my-export-2/0001-01-01T00:00:02.000Z/0001-01-01T00:00:02.000Z
-----
%A
>>>>>

<<<<<
slice/active/opened/writing/00000123/my-receiver-A/my-export-1/%s
%A
>>>>>

<<<<<
slice/active/opened/writing/00000123/my-receiver-B/my-export-2/0001-01-01T00:00:02.000Z/%s
%A
>>>>>

<<<<<
stats/received/00000123/my-receiver-A/my-export-1/0001-01-01T00:00:02.000Z/0001-01-01T00:00:02.000Z/api-node-1
%A
>>>>>

<<<<<
stats/received/00000123/my-receiver-B/my-export-2/0001-01-01T00:00:02.000Z/0001-01-01T00:00:02.000Z/api-node-1
%A
>>>>>

<<<<<
task/00000123/my-receiver-A/my-export-1/file.swap/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-A",
  "exportId": "my-export-1",
  "type": "file.swap",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node-2",
  "lock": "file.swap/%s",
  "result": "new file created, the old is closing",
  "duration": 0
}
>>>>>

<<<<<
task/00000123/my-receiver-A/my-export-1/slice.swap/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-A",
  "exportId": "my-export-1",
  "type": "slice.swap",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node-2",
  "lock": "slice.swap/%s",
  "result": "new slice created, the old is closing",
  "duration": 0
}
>>>>>

<<<<<
task/00000123/my-receiver-B/my-export-2/slice.swap/%s
-----
{
  "projectId": 123,
  "receiverId": "my-receiver-B",
  "exportId": "my-export-2",
  "type": "slice.swap",
  "createdAt": "%s",
  "randomId": "%s",
  "finishedAt": "%s",
  "workerNode": "worker-node-1",
  "lock": "slice.swap/%s",
  "result": "new slice created, the old is closing",
  "duration": 0
}
>>>>>
`)
}

// createExport creates receiver,export,mapping,file and slice.
func createExport2(t *testing.T, receiverID, exportID string, ctx context.Context, clk clock.Clock, client *etcd.Client, str *store.Store, fileRes *storageapi.File, importConditions model.Conditions, token string) key.SliceKey {
	t.Helper()
	receiver := model.ReceiverForTest(receiverID, 0, clk.Now())
	columns := []column.Column{
		column.ID{Name: "col01"},
		column.Datetime{Name: "col02"},
		column.IP{Name: "col03"},
		column.Body{Name: "col04"},
		column.Headers{Name: "col05"},
		column.Template{Name: "col06", Language: "jsonnet", Content: `"---" + Body("key") + "---"`},
	}
	export := model.ExportForTest(receiver.ReceiverKey, exportID, "in.c-bucket.table", columns, clk.Now())
	export.Token.Token = token
	export.ImportConditions = importConditions

	if fileRes != nil {
		export.OpenedFile.StorageResource = fileRes
		export.OpenedSlice.StorageResource = fileRes
	}

	etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.CreateReceiver(ctx, receiver))
		assert.NoError(t, str.CreateExport(ctx, export))
	})
	return export.OpenedSlice.SliceKey
}
