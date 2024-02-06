package service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/column"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestCleanup(t *testing.T) {
	t.Parallel()

	project := testproject.GetTestProjectForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)

	// Create nodes
	clk := clock.NewMock()
	clk.Set(time.Now())
	clk.Add(time.Second)
	opts := []dependencies.MockedOption{
		dependencies.WithEnabledOrchestrator(),
		dependencies.WithClock(clk),
		dependencies.WithEtcdConfig(etcdCfg),
		dependencies.WithTestProject(project),
	}

	// Create receivers, exports and records
	cleanupInterval := 2 * time.Second
	workerScp, workerMock := bufferDependencies.NewMockedWorkerScope(
		t,
		config.NewWorkerConfig().Apply(
			config.WithConditionsCheck(false),
			config.WithCloseSlices(false),
			config.WithUploadSlices(false),
			config.WithRetryFailedSlices(false),
			config.WithCloseFiles(false),
			config.WithImportFiles(false),
			config.WithRetryFailedFiles(false),
			config.WithCleanup(true),
			config.WithCleanupInterval(cleanupInterval),
		),
		append(opts, dependencies.WithNodeID("worker-node-1"))...,
	)

	store := workerScp.Store()
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	export1 := model.ExportForTest(receiverKey, "first", "in.c-bucket.table", []column.Column{column.ID{Name: "col01"}}, clk.Now().AddDate(0, -1, 0))
	export2 := model.ExportForTest(receiverKey, "another", "in.c-bucket.table", []column.Column{column.ID{Name: "col01"}}, clk.Now())
	receiver := model.Receiver{
		ReceiverBase: model.ReceiverBase{
			ReceiverKey: receiverKey,
			Name:        "rec1",
			Secret:      "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		},
		Exports: []model.Export{
			export1,
			export2,
		},
	}
	err := store.CreateReceiver(ctx, receiver)
	assert.NoError(t, err)

	// Open new file in export1
	oldFile, oldSlice := export1.OpenedFile, export1.OpenedSlice
	fileKey := key.FileKey{ExportKey: export1.ExportKey, FileID: key.FileID(clk.Now())}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now())}
	export1.OpenedFile = model.File{
		FileKey:         fileKey,
		State:           filestate.Opened,
		Mapping:         export1.Mapping,
		StorageResource: &keboola.FileUploadCredentials{},
	}
	export1.OpenedSlice = model.Slice{
		SliceKey:        sliceKey,
		State:           slicestate.Writing,
		Mapping:         export1.Mapping,
		StorageResource: &keboola.FileUploadCredentials{},
		Number:          1,
	}
	assert.NoError(t, store.SwapFile(ctx, &oldFile, &oldSlice, export1.OpenedFile, export1.OpenedSlice))

	// Create nodes
	_, err = service.New(workerScp)
	assert.NoError(t, err)

	// Trigger cleanup
	clk.Add(cleanupInterval)

	// Wait for the cleanup task
	assert.Eventually(t, func() bool {
		return strings.Contains(workerMock.DebugLogger().AllMessages(), "task succeeded")
	}, 10*time.Second, 100*time.Millisecond)

	// Shutdown
	workerScp.Process().Shutdown(ctx, errors.New("bye bye Worker 1"))
	workerScp.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
[task][_system_/tasks.cleanup/%s]INFO  started task
[task][_system_/tasks.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/tasks.cleanup"
[task][_system_/tasks.cleanup/%s]INFO  deleted "0" tasks
[task][_system_/tasks.cleanup/%s]INFO  task succeeded (0s): deleted "0" tasks
[task][_system_/tasks.cleanup/%s]DEBUG  lock released "runtime/lock/task/tasks.cleanup"
	`, strhelper.FilterLines(`^\[task\]\[_system_/`, workerMock.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
[service][cleanup]INFO  ready
[service][cleanup]INFO  started "1" receiver cleanup tasks
	`, strhelper.FilterLines(`^\[service\]\[cleanup\]`, workerMock.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
[task][1000/github/receiver.cleanup/%s]INFO  started task
[task][1000/github/receiver.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/1000/github/receiver.cleanup"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted slice "1000/github/first/%s"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted file "1000/github/first/%s"
[task][1000/github/receiver.cleanup/%s]INFO  deleted "1" files, "1" slices, "0" records
[task][1000/github/receiver.cleanup/%s]INFO  task succeeded (%s): receiver "1000/github" has been cleaned
[task][1000/github/receiver.cleanup/%s]DEBUG  lock released "runtime/lock/task/1000/github/receiver.cleanup"
	`, strhelper.FilterLines(`^\[task\]\[1000/github/receiver.cleanup`, workerMock.DebugLogger().AllMessages()))

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
config/export/1000/github/another
-----
%A
>>>>>

<<<<<
config/export/1000/github/first
-----
%A
>>>>>

<<<<<
config/mapping/revision/1000/github/another/00000001
-----
%A
>>>>>

<<<<<
config/mapping/revision/1000/github/first/00000001
-----
%A
>>>>>

<<<<<
config/receiver/1000/github
-----
%A
>>>>>

<<<<<
file/opened/1000/github/first/%s
-----
%A
>>>>>

<<<<<
file/opened/1000/github/another/%s
-----
%A
>>>>>

<<<<<
secret/export/token/1000/github/another
-----
%A
>>>>>

<<<<<
secret/export/token/1000/github/first
-----
%A
>>>>>

<<<<<
slice/active/opened/writing/1000/github/first/%s
-----
%A
>>>>>

<<<<<
slice/active/opened/writing/1000/github/another/%s
-----
%A
>>>>>

<<<<<
task/1000/github/receiver.cleanup/%s
-----
{
  "projectId": 1000,
  "taskId": "github/receiver.cleanup/%s",
  "type": "receiver.cleanup",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "%s",
  "lock": "runtime/lock/task/1000/github/receiver.cleanup",
  "result": "receiver \"1000/github\" has been cleaned",
  "duration": %d
}
>>>>>

<<<<<
task/_system_/tasks.cleanup/%s
-----
{
  "systemTask": true,
  "taskId": "tasks.cleanup/%s",
  "type": "tasks.cleanup",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "worker-node-1",
  "lock": "runtime/lock/task/tasks.cleanup",
  "result": "deleted \"0\" tasks",
  "duration": %d
}
>>>>>
`)
}
