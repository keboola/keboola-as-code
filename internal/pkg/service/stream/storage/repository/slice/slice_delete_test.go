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
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"testing"
	"time"
)

func TestSliceRepository_DeleteSliceOnFileDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

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

	// Create parent branch, source and sink (with the first file)
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey model.FileKey
	var sliceKey1 model.SliceKey
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewKeboolaTableSink(sinkKey)
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
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.Rotate(clk.Now(), sliceKey1).Do(ctx).Err())
	}

	// Delete file, it in cascade deletes both slices
	// -----------------------------------------------------------------------------------------------------------------
	//var deleteEtcdLogs string
	{
		require.NoError(t, fileRepo.Delete(fileKey, clk.Now()).Do(ctx).Err())
		//deleteEtcdLogs = etcdLogs.String()
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	// etcdlogger.AssertFromFile(t, `fixtures/file_delete_ops_001.txt`, deleteEtcdLogs)

	// Check etcd state - there is no file
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, ``, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/secret/token/|storage/volume"))
}
