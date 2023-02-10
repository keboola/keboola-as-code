package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
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

func TestCleanup(t *testing.T) {
	t.Parallel()

	project := testproject.GetTestProjectForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create nodes
	clk := clock.NewMock()
	clk.Set(time.Now())
	clk.Add(time.Second)
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{
		dependencies.WithClock(clk),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	// Create receivers, exports and records
	workerDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("worker-node-1"))...)
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
	cleanupInterval := 2 * time.Second
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	serviceOps := []service.Option{
		service.WithCheckConditions(false),
		service.WithCloseSlices(false),
		service.WithUploadSlices(false),
		service.WithRetryFailedSlices(false),
		service.WithCloseFiles(false),
		service.WithImportFiles(false),
		service.WithRetryFailedFiles(false),
		service.WithCleanup(true),
		service.WithCleanupInterval(cleanupInterval),
	}
	_, err = service.New(workerDeps, serviceOps...)
	assert.NoError(t, err)

	time.Sleep(time.Second)
	clk.Add(cleanupInterval)

	// Shutdown
	time.Sleep(2 * time.Second)
	workerDeps.Process().Shutdown(errors.New("bye bye Worker 1"))
	workerDeps.Process().WaitForShutdown()

	// Check cleanup logs
	wildcards.Assert(t, `
[service][cleanup]INFO  deleting file "00001000/github/first/%s"
	`, strhelper.FilterLines(`^(\[service\]\[cleanup\])`, workerDeps.DebugLogger().AllMessages()))

	etcdhelper.AssertKVs(t, client, `
<<<<<
config/export/00001000/github/another
-----
%A
>>>>>

<<<<<
config/export/00001000/github/first
-----
%A
>>>>>

<<<<<
config/mapping/revision/00001000/github/another/00000001
-----
%A
>>>>>

<<<<<
config/mapping/revision/00001000/github/first/00000001
-----
%A
>>>>>

<<<<<
config/receiver/00001000/github
-----
%A
>>>>>

<<<<<
file/opened/00001000/github/another/%s
-----
%A
>>>>>

<<<<<
secret/export/token/00001000/github/another
-----
%A
>>>>>

<<<<<
secret/export/token/00001000/github/first
-----
%A
>>>>>

<<<<<
slice/active/opened/writing/00001000/github/another/%s
-----
%A
>>>>>`)
}
