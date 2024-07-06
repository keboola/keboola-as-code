package slice_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestSliceRepository_StateTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	by := test.ByUser()

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
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

	// Create parent branch, source, sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	var file model.File
	var slice model.Slice
	{
		var err error
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		files, err := fileRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, files, 1)
		file = files[0]
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice = slices[0]
	}

	// Switch slice to the storage.SliceClosing state by file rotate
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())
	}

	// Switch slice to the storage.SliceUploading state
	// -----------------------------------------------------------------------------------------------------------------
	// var toUploadingEtcdLogs string
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(slice.SliceKey, clk.Now()).Do(ctx).Err())
	}

	// Switch slice to the storage.SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	var toUploadedEtcdLogs string
	{
		clk.Add(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, sliceRepo.SwitchToUploaded(slice.SliceKey, clk.Now()).Do(ctx).Err())
		toUploadedEtcdLogs = etcdLogs.String()
	}

	// Switch slice to the storage.SliceImported state, together with the file to the storage.FileImported state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImporting(file.FileKey, clk.Now()).Do(ctx).Err())
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImported(file.FileKey, clk.Now()).Do(ctx).Err())
	}

	// Check etcd operations
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.AssertFromFile(t, `fixtures/slice_state_ops_001.txt`, toUploadedEtcdLogs)

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsFromFile(t, client, `fixtures/slice_state_snapshot_001.txt`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/slice/all/|storage/secret/|storage/volume/"))
}
