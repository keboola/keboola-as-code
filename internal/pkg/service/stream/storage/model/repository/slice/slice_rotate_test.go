package slice_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/slice"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestSliceRepository_Rotate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
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

	// Create parent branch, source, sink, token, file and slice
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey model.SliceKey
	{
		var err error
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice := slices[0]
		assert.Equal(t, clk.Now(), slice.OpenedAt().Time())
		sliceKey = slice.SliceKey
	}

	// Create the second slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdLogs.Reset()
		clk.Advance(time.Hour)
		slice2, err := sliceRepo.Rotate(sliceKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice2.OpenedAt().Time())
	}

	// Create the third file
	// -----------------------------------------------------------------------------------------------------------------
	var rotateEtcdLogs string
	{
		var err error
		clk.Advance(time.Hour)
		etcdLogs.Reset()
		slice3, err := sliceRepo.Rotate(sliceKey, clk.Now()).Do(ctx).ResultOrErr()
		rotateEtcdLogs = etcdLogs.String()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice3.OpenedAt().Time())
	}

	// Check etcd operations
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.AssertFromFile(t, `fixtures/slice_rotate_ops_001.txt`, rotateEtcdLogs)

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/slice_rotate_snapshot_001.txt", etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/slice/all/|storage/stats/|storage/secret/|storage/volume"))
}

func TestSliceRepository_Rotate_WithClosingFile(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
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

	// Create parent branch, source, sink, token, file and slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Rotate - create the second file
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Advance(time.Hour)
		etcdLogs.Reset()
		file2, err := storageRepo.File().Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file2.OpenedAt().Time())
	}

	// Try to rotate slice while File is already closing
	// -----------------------------------------------------------------------------------------------------------------
	{
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 2)
		sliceKey := slices[0].SliceKey
		etcdLogs.Reset()
		_, err = sliceRepo.Rotate(sliceKey, clk.Now()).Do(ctx).ResultOrErr()
		require.ErrorAs(t, err, &slice.UnexpectedFileSliceStatesError{})
	}
}
