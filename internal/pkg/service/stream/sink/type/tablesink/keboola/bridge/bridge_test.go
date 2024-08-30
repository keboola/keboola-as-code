package bridge_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	bridgeTest "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/test"
	staging "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	target "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestBridge_FullWorkflow(t *testing.T) {
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
	ignoredKeys := etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/volume/")

	// Get services
	d, mocked := dependencies.NewMockedAPIScopeWithConfig(t, ctx, func(cfg *config.Config) {
		// Lock configuration, so it is not affected by the default values
		cfg.Storage.Level.Staging.Upload.Trigger = staging.UploadTrigger{
			Count:    10000,
			Size:     1 * datasize.MB,
			Interval: duration.From(1 * time.Minute),
		}
		cfg.Storage.Level.Target.Import.Trigger = target.ImportTrigger{
			Count:       50000,
			Size:        5 * datasize.MB,
			Interval:    duration.From(5 * time.Minute),
			SlicesCount: 100,
			Expiration:  duration.From(30 * time.Minute),
		}
	}, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// apiCtx - for operations triggered by an authorized API call
	apiCtx := rollback.ContextWith(ctx, rollback.New(d.Logger()))
	apiCtx = context.WithValue(apiCtx, dependencies.KeboolaProjectAPICtxKey, mocked.KeboolaProjectAPI())

	// Register mocked responses
	// -----------------------------------------------------------------------------------------------------------------
	transport := mocked.MockedHTTPTransport()
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

	// Create parent branch, source, sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(apiCtx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(apiCtx).Err())
	}

	// Create sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	var createSinkEtcdLogs string
	{
		sink := test.NewKeboolaTableSink(sinkKey)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(apiCtx).Err())
		createSinkEtcdLogs = etcdLogs.String()
	}
	{
		// Storage API calls
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/tokens"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets/in.c-bucket/tables-definition"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/tables/in.c-bucket.my-table/metadata"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/files/prepare"])
		transport.ZeroCallCounters()
	}
	{
		// Logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"created bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"creating token","token.bucketID":"in.c-bucket"}
{"level":"info","message":"created token","token.bucketID":"in.c-bucket","token.name":"[_internal] Stream Sink my-source/my-sink"}
{"level":"info","message":"creating table","table.key":"456/in.c-bucket.my-table"}
{"level":"info","message":"created table","table.key":"456/in.c-bucket.my-table"}
{"level":"info","message":"creating staging file","token.ID":"1001","file.name":"my-source_my-sink_20000101010000","file.id":"2000-01-01T01:00:00.000Z"}
{"level":"info","message":"created staging file","token.ID":"1001","file.resourceID":"1001","file.name":"my-source_my-sink_20000101010000","file.id":"2000-01-01T01:00:00.000Z"}
`)
		mocked.DebugLogger().Truncate()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/create_sink_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/create_sink_ops.txt", createSinkEtcdLogs)
	}

	// Disable sink - token is deleted
	// -----------------------------------------------------------------------------------------------------------------
	var disableSinkEtcdLogs string
	{
		clk.Add(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(apiCtx).Err())
		disableSinkEtcdLogs = etcdLogs.String()
	}
	{
		// Storage API calls
		assert.Equal(t, 1, transport.GetCallCountInfo()["DELETE https://connection.keboola.local/v2/storage/tokens/1001"])
		transport.ZeroCallCounters()
	}
	{
		// Logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"deleting token","token.ID":"1001"}
{"level":"info","message":"deleted token","token.ID":"1001"}
`)
		mocked.DebugLogger().Truncate()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/disable_sink_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/disable_sink_ops.txt", disableSinkEtcdLogs)
	}

	// Enable sink - new token and file are created
	// -----------------------------------------------------------------------------------------------------------------
	var enableSinkEtcdLogs string
	{
		clk.Add(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Enable(sinkKey, clk.Now(), by).Do(apiCtx).Err())
		enableSinkEtcdLogs = etcdLogs.String()
	}
	{
		// Storage API calls
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/tokens"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/files/prepare"])
		transport.ZeroCallCounters()
	}
	{
		// Logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating token","token.bucketID":"in.c-bucket"}
{"level":"info","message":"created token","token.bucketID":"in.c-bucket","token.name":"[_internal] Stream Sink my-source/my-sink"}
{"level":"info","message":"creating staging file","token.ID":"1002","file.name":"my-source_my-sink_20000101030000","file.id":"2000-01-01T03:00:00.000Z"}
{"level":"info","message":"created staging file","token.ID":"1002","file.resourceID":"1002","file.name":"my-source_my-sink_20000101030000","file.id":"2000-01-01T03:00:00.000Z"}
`)
		mocked.DebugLogger().Truncate()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/enable_sink_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/enable_sink_ops.txt", enableSinkEtcdLogs)
	}

	// Delete files - simulate periodical cleanup, upload credentials are deleted too
	// -----------------------------------------------------------------------------------------------------------------
	var deleteFilesEtcdLogs string
	{
		clk.Add(time.Hour)
		fileKey1 := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.MustParse("2000-01-01T01:00:00.000Z")}}
		fileKey2 := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.MustParse("2000-01-01T03:00:00.000Z")}}
		etcdLogs.Reset()
		require.NoError(t, fileRepo.Delete(fileKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.Delete(fileKey2, clk.Now()).Do(ctx).Err())
		deleteFilesEtcdLogs = etcdLogs.String()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/delete_files_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/delete_files_ops.txt", deleteFilesEtcdLogs)
	}
}
