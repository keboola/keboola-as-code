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

	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	bridgeTest "github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestBridge(t *testing.T) {
	t.Parallel()

	// bgCtx - for internal background operations
	bgCtx := context.Background()

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
	d, mocked := dependencies.NewMockedLocalStorageScope(t, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	volumeRepo := storageRepo.Volume()

	// apiCtx - for operations triggered by an authorized API call
	apiCtx := rollback.ContextWith(bgCtx, rollback.New(d.Logger()))
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
		test.RegisterWriterVolumes(t, bgCtx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sinks
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(apiCtx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(apiCtx).Err())
		sink1 := test.NewKeboolaTableSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), by, "Create sink").Do(apiCtx).Err())
	}
	{
		// Check created Storage API resources
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/create_resources_snapshot_001.txt", ignoredKeys)
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/tokens"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets/in.c-bucket/tables-definition"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/files/prepare"])
		transport.ZeroCallCounters()
	}
	{
		// Check logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"created bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"creating table","table.key":"456/in.c-bucket.table"}
{"level":"info","message":"created table","table.key":"456/in.c-bucket.table"}
{"level":"info","message":"creating token","token.bucketID":"in.c-bucket"}
{"level":"info","message":"created token","token.bucketID":"in.c-bucket","token.name":"[_internal] Stream Sink my-source/my-sink"}
{"level":"info","message":"creating staging file","token.ID":"1001","file.name":"my-source_my-sink_20000101010000","file.key":"123/456/my-source/my-sink/2000-01-01T01:00:00.000Z"}
{"level":"info","message":"created staging file","token.ID":"1001","file.resourceID":"1001","file.name":"my-source_my-sink_20000101010000","file.key":"123/456/my-source/my-sink/2000-01-01T01:00:00.000Z"}
`)
		mocked.DebugLogger().Truncate()
	}

	// Disable sink - token is deleted
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(apiCtx).Err())
	}
	{
		// Check deleted token
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/create_resources_snapshot_002.txt", ignoredKeys)
		assert.Equal(t, 1, transport.GetCallCountInfo()["DELETE https://connection.keboola.local/v2/storage/tokens/1001"])
		transport.ZeroCallCounters()
	}
	{
		// Check logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"deleting token","token.ID":"1001"}
{"level":"info","message":"deleted token","token.ID":"1001"}
`)
		mocked.DebugLogger().Truncate()
	}

	// Enable sink - new token and file are created
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, defRepo.Sink().Enable(sinkKey, clk.Now(), by).Do(apiCtx).Err())
	}
	{
		// Check created Storage API resources
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/create_resources_snapshot_003.txt", ignoredKeys)
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/tokens"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/files/prepare"])
		transport.ZeroCallCounters()
	}
	{
		// Check logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating token","token.bucketID":"in.c-bucket"}
{"level":"info","message":"created token","token.bucketID":"in.c-bucket","token.name":"[_internal] Stream Sink my-source/my-sink"}
{"level":"info","message":"creating staging file","token.ID":"1002","file.name":"my-source_my-sink_20000101030000","file.key":"123/456/my-source/my-sink/2000-01-01T03:00:00.000Z"}
{"level":"info","message":"created staging file","token.ID":"1002","file.resourceID":"1002","file.name":"my-source_my-sink_20000101030000","file.key":"123/456/my-source/my-sink/2000-01-01T03:00:00.000Z"}
`)
		mocked.DebugLogger().Truncate()
	}

	// Delete files - simulate periodical cleanup, upload credentials are deleted too
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		fileKey1 := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.MustParse("2000-01-01T01:00:00.000Z")}}
		fileKey2 := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.MustParse("2000-01-01T03:00:00.000Z")}}
		require.NoError(t, fileRepo.Delete(fileKey1, clk.Now()).Do(bgCtx).Err())
		require.NoError(t, fileRepo.Delete(fileKey2, clk.Now()).Do(bgCtx).Err())
	}
	{
		// Check delete upload credentials
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/create_resources_snapshot_004.txt", ignoredKeys)
	}
}
