package file_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestFileRepository_CloseFileOnSourceDeactivation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
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

	// Create sinks, it triggers files creation
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink1 := test.NewSinkWithLocalStorage(sinkKey1)
		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), by, "Create sink").Do(ctx).Err())
		sink2 := test.NewSinkWithLocalStorage(sinkKey2)
		require.NoError(t, defRepo.Sink().Create(&sink2, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Disable Source, it in cascade disables Sinks, it triggers files closing
	// -----------------------------------------------------------------------------------------------------------------
	// var closeEtcdLogs string
	{
		clk.Add(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Source().Disable(sourceKey, clk.Now(), by, "test reason").Do(ctx).Err())
		// closeEtcdLogs = etcdLogs.String()
	}

	// Check etcd operations
	// -----------------------------------------------------------------------------------------------------------------
	// etcdlogger.AssertFromFile(t, `fixtures/file_close_ops_001.txt`, closeEtcdLogs)

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/file_close_snapshot_001.txt", etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/|storage/secret/|storage/volume/"))
}

func TestFileRepository_CloseFileOnSinkDeactivation(t *testing.T) {
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

	// Create sink, it triggers file creation
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Disable Sink, it in cascade disables Sinks, it triggers files closing
	// -----------------------------------------------------------------------------------------------------------------
	// var closeEtcdLogs string
	{
		clk.Add(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "test reason").Do(ctx).Err())
		// closeEtcdLogs = etcdLogs.String()
	}

	// Check etcd operations
	// -----------------------------------------------------------------------------------------------------------------
	// etcdlogger.AssertFromFile(t, `fixtures/file_close_ops_002.txt`, closeEtcdLogs)

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/file_close_snapshot_002.txt", etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/|storage/secret/|storage/volume/"))
}
