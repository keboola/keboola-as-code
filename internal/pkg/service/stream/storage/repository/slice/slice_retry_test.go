package slice_test

import (
	"bytes"
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"testing"
	"time"
)

func TestSliceRepository_IncrementRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
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

	// Create parent branch, source, sink, token, file and slice1
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey model.SliceKey
	{
		var err error
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewKeboolaTableSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice := slices[0]
		assert.Equal(t, clk.Now(), slice.OpenedAt().Time())
		sliceKey = slice.SliceKey
	}

	// Rotate
	// -----------------------------------------------------------------------------------------------------------------
	// var rotateEtcdLogs string
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.Rotate(clk.Now(), sliceKey).Do(ctx).Err())
	}

	// Switch the slice to the Uploading state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey, clk.Now()).Do(ctx).Err())
	}

	// Upload failed, increment retry attempt
	// -----------------------------------------------------------------------------------------------------------------
	{
		var err error
		clk.Add(time.Hour)
		slice, err := sliceRepo.IncrementRetry(clk.Now(), sliceKey, "some reason 1").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploading, slice.State)
	}

	// Upload failed again, increment retry attempt
	// -----------------------------------------------------------------------------------------------------------------
	{
		var err error
		clk.Add(time.Hour)
		slice, err := sliceRepo.IncrementRetry(clk.Now(), sliceKey, "some reason 2").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.SliceUploading, slice.State)
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	// etcdlogger.Assert(t, ``, rotateEtcdLogs)

	// Check etcd state
	//   - Only the last slice per file and volume is in the storage.SliceWriting state.
	//   - Other slices per file and volume are in the storage.SlicesClosing state.
	//   - AllocatedDiskSpace of the slice5 is 330MB it is 110% of the slice3.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/slice_retry_snapshot_002.txt", etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/slice/all/|storage/stats/|storage/secret/token/|storage/volume"))
}
