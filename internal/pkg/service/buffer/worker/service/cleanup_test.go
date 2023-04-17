package service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	workerConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestCleanup(t *testing.T) {
	t.Parallel()

	project := testproject.GetTestProjectForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create nodes
	clk := clock.NewMock()
	clk.Set(time.Now())
	clk.Add(time.Second)
	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	// Create receivers, exports and records
	cleanupInterval := 2 * time.Second
	workerDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("worker-node-1"))...)
	workerDeps.SetWorkerConfigOps(
		workerConfig.WithConditionsCheck(false),
		workerConfig.WithCloseSlices(false),
		workerConfig.WithUploadSlices(false),
		workerConfig.WithRetryFailedSlices(false),
		workerConfig.WithCloseFiles(false),
		workerConfig.WithImportFiles(false),
		workerConfig.WithRetryFailedFiles(false),
		workerConfig.WithCleanup(true),
		workerConfig.WithCleanupInterval(cleanupInterval),
	)
	store := workerDeps.Store()
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

	// Create nodes
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err = service.New(workerDeps)
	assert.NoError(t, err)

	// Trigger cleanup
	clk.Add(cleanupInterval)

	// Wait for the cleanup task
	assert.Eventually(t, func() bool {
		return strings.Contains(workerDeps.DebugLogger().AllMessages(), "task succeeded")
	}, 10*time.Second, 100*time.Millisecond)

	// Shutdown
	workerDeps.Process().Shutdown(errors.New("bye bye Worker 1"))
	workerDeps.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
[task][cleanup]DEBUG  lock acquired "runtime/lock/task/tasks.cleanup"
[task][cleanup]INFO  deleted "0" tasks
[task][cleanup]DEBUG  lock released "runtime/lock/task/tasks.cleanup"
	`, strhelper.FilterLines(`^\[task\]\[cleanup\]`, workerDeps.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
[service][cleanup]INFO  ready
[service][cleanup]INFO  started "1" receiver cleanup tasks
	`, strhelper.FilterLines(`^\[service\]\[cleanup\]`, workerDeps.DebugLogger().AllMessages()))
	wildcards.Assert(t, `
[task][1000/github/receiver.cleanup/%s]INFO  started task
[task][1000/github/receiver.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/1000/github/receiver.cleanup"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted slice "1000/github/first/%s"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted file "1000/github/first/%s"
[task][1000/github/receiver.cleanup/%s]INFO  deleted "1" files, "1" slices, "0" records
[task][1000/github/receiver.cleanup/%s]INFO  task succeeded (%s): receiver "1000/github" has been cleaned
[task][1000/github/receiver.cleanup/%s]DEBUG  lock released "runtime/lock/task/1000/github/receiver.cleanup"
	`, strhelper.FilterLines(`^\[task\]\[1000/github/receiver.cleanup`, workerDeps.DebugLogger().AllMessages()))

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
`)
}
