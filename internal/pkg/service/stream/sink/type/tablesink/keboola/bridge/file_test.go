package bridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	bridgeTest "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/fileimport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestBridge_ImportFile_EmptyFile(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	checkInterval := time.Minute
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Operator.FileImportCheckInterval = duration.From(checkInterval)
	}, commonDeps.WithClock(clk))
	logger := mock.DebugLogger()
	client := mock.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()

	apiCtx := rollback.ContextWith(ctx, rollback.New(d.Logger()))
	apiCtx = context.WithValue(apiCtx, dependencies.KeboolaProjectAPICtxKey, mock.KeboolaProjectAPI())

	// Register mocked responses
	// -----------------------------------------------------------------------------------------------------------------
	transport := mock.MockedHTTPTransport()
	{
		bridgeTest.MockTokenStorageAPICalls(t, transport)
		bridgeTest.MockBucketStorageAPICalls(t, transport)
		bridgeTest.MockTableStorageAPICalls(t, transport)
		bridgeTest.MockFileStorageAPICalls(t, clk, transport)
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create sink
	// -----------------------------------------------------------------------------------------------------------------
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(apiCtx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(apiCtx).Err())
		sink := test.NewKeboolaTableSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(apiCtx).Err())
	}

	// Switch file to the FileImporting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Rotate file
		clk.Add(time.Second)
		require.NoError(t, storageRepo.File().Rotate(sinkKey, clk.Now()).Do(apiCtx).Err())
		files, err := storageRepo.File().ListAll().Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, files, 2)
		slices, err := storageRepo.Slice().ListIn(files[0].FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		require.Equal(t, model.FileClosing, files[0].State)
		require.Equal(t, model.FileWriting, files[1].State)
		require.Equal(t, model.SliceClosing, slices[0].State)
		file := files[0]
		slice := slices[0]

		// Simulate upload of an empty slice
		clk.Add(time.Second)
		require.NoError(t, storageRepo.Slice().SwitchToUploading(slice.SliceKey, clk.Now(), true).Do(ctx).Err())
		require.NoError(t, storageRepo.Slice().SwitchToUploaded(slice.SliceKey, clk.Now()).Do(ctx).Err())

		// Switch file to the FileImporting state
		clk.Add(time.Second)
		require.NoError(t, storageRepo.File().SwitchToImporting(file.FileKey, clk.Now(), true).Do(ctx).Err())
	}

	// Start file import operator
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, fileimport.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	// Wait import of the empty file
	// -----------------------------------------------------------------------------------------------------------------
	logger.Truncate()
	transport.ZeroCallCounters()
	clk.Add(checkInterval)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"importing file","component":"storage.node.operator.file.import"}
{"level":"info","message":"empty file, skipped import, deleting empty staging file","component":"keboola.bridge"}
{"level":"info","message":"imported file","component":"storage.node.operator.file.import"}
`)
	}, 15*time.Second, 50*time.Millisecond)

	// Empty file in the Storage API has been deleted
	// -----------------------------------------------------------------------------------------------------------------
	expectedStorageAPICall := "DELETE https://connection.keboola.local/v2/storage/branch/456/files/1001"
	assert.Equal(t, 1, transport.GetCallCountInfo()[expectedStorageAPICall])

	// Shutdown
	// -----------------------------------------------------------------------------------------------------------------
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()
	logger.AssertNoErrorMessage(t)
}
