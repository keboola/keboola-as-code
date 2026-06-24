package slice_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestSliceRepository_DeleteSliceOnFileDelete(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Get services
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source and sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey model.FileKey
	var sliceKey1 model.SliceKey
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		fileKey = model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.From(clk.Now())}}
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		sliceKey1 = slices[0].SliceKey
	}

	// Rotate - create the second slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Advance(time.Hour)
		require.NoError(t, sliceRepo.Rotate(sliceKey1, clk.Now()).Do(ctx).Err())
	}

	// Delete file, it in cascade deletes both slices
	// -----------------------------------------------------------------------------------------------------------------
	var deleteEtcdLogs string
	{
		etcdLogs.Reset()
		require.NoError(t, fileRepo.Delete(fileKey, clk.Now()).Do(ctx).Err())
		deleteEtcdLogs = etcdLogs.String()
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.AssertFromFile(t, `fixtures/slice_delete_ops_001.txt`, deleteEtcdLogs)

	// Check etcd state - there is no file
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, ``, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/secret/|storage/volume"))
}

// TestSliceRepository_DeleteSliceOnFileDelete_BoundedTxnSize exercises end-to-end that a file with
// many slices can be deleted: the slices are deleted in bounded batches (op.RunWriteTxnsInBatches)
// rather than a single transaction. The batch-sizing itself is verified limit-independently in the
// op package (TestRunWriteTxnsInBatches); this test confirms the file-delete cascade uses it.
func TestSliceRepository_DeleteSliceOnFileDelete_BoundedTxnSize(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// More slices than fit in a single batch, so the delete must span multiple transactions.
	const sliceCount = 70

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()

	// Register active volumes
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create branch, source, sink (opens the file with one slice)
	var fileKey model.FileKey
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		fileKey = model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.From(clk.Now())}}
	}

	// Rotate slices to grow the file up to sliceCount slices
	{
		slices, err := sliceRepo.ListIn(fileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		current := slices[0].SliceKey
		for range sliceCount - 1 {
			clk.Advance(time.Hour)
			require.NoError(t, sliceRepo.Rotate(current, clk.Now()).Do(ctx).Err())
			latest, err := sliceRepo.ListIn(fileKey).Do(ctx).All()
			require.NoError(t, err)
			current = latest[len(latest)-1].SliceKey
		}
		all, err := sliceRepo.ListIn(fileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, all, sliceCount)
	}

	// Delete the file - must succeed despite the large number of slices
	clk.Advance(time.Hour)
	require.NoError(t, fileRepo.Delete(fileKey, clk.Now()).Do(ctx).Err())

	// All slices must be gone
	remaining, err := sliceRepo.ListIn(fileKey).Do(ctx).All()
	require.NoError(t, err)
	require.Empty(t, remaining)
}
