package repository_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestFileRepository_RotateAllIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T02:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey1 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sourceKey2 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-2"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-3"}
	sinkKey4 := key.SinkKey{SourceKey: sourceKey2, SinkID: "my-sink-4"}
	sinkKey5 := key.SinkKey{SourceKey: sourceKey2, SinkID: "my-sink-5"}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	test.MockCreateFilesStorageAPICalls(t, clk, branchKey, transport)
	test.MockDeleteFilesStorageAPICalls(t, branchKey, transport)

	// Create parent branch, source, sinks and tokens
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(clk.Now(), &branch).Do(ctx).Err())
		source1 := test.NewSource(sourceKey1)
		require.NoError(t, defRepo.Source().Create(clk.Now(), "Create source", &source1).Do(ctx).Err())
		source2 := test.NewSource(sourceKey2)
		require.NoError(t, defRepo.Source().Create(clk.Now(), "Create source", &source2).Do(ctx).Err())
		sink1 := test.NewSink(sinkKey1)
		sink1.Config = sink1.Config.With(testconfig.LocalVolumeConfig(3, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink1).Do(ctx).Err())
		sink2 := test.NewSink(sinkKey2)
		sink2.Config = sink2.Config.With(testconfig.LocalVolumeConfig(3, []string{"hdd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink2).Do(ctx).Err())
		sink3 := test.NewSink(sinkKey3)
		sink3.Config = sink3.Config.With(testconfig.LocalVolumeConfig(2, []string{"ssd", "hdd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink3).Do(ctx).Err())
		sink4 := test.NewSink(sinkKey4)
		sink4.Config = sink4.Config.With(testconfig.LocalVolumeConfig(1, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink4).Do(ctx).Err())
		sink5 := test.NewSink(sinkKey5)
		sink5.Config = sink5.Config.With(testconfig.LocalVolumeConfig(1, []string{"hdd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink5).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink1.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink2.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink3.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink4.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink5.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 5)
	}

	// Create (the first file Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	var rotateAllInEtcdLogs string
	{
		clk.Add(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, fileRepo.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).Err())
		rotateAllInEtcdLogs = etcdLogs.String()
	}

	// Rotate file (2)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).Err())
	}

	// Rotate file (3)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).Err())
	}

	// Check Storage API calls
	// -----------------------------------------------------------------------------------------------------------------
	// File prepare endpoint should be called N times
	assert.Equal(t, 15, transport.GetCallCountInfo()["POST /v2/storage/branch/456/files/prepare"])
	assert.Equal(t, 0, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Test rollback, delete file endpoint should be called N times
	rb.Invoke(ctx)
	assert.Equal(t, 15, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// RotateAllIn - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  // Sinks
  001 ➡️  GET ["definition/sink/active/123/456/", "definition/sink/active/123/4560")
  // Volumes
  002 ➡️  GET ["storage/volume/writer/", "storage/volume/writer0")
  // Local files
  003 ➡️  GET ["storage/file/level/local/123/456/", "storage/file/level/local/123/4560")
  // Local slices
  004 ➡️  GET ["storage/slice/level/local/123/456/", "storage/slice/level/local/123/4560")
✔️  TXN | succeeded: true

// RotateAllIn - FileResourcesProvider - Get tokens
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET "storage/secret/token/123/456/my-source-1/my-sink-1"
  002 ➡️  GET "storage/secret/token/123/456/my-source-1/my-sink-2"
  003 ➡️  GET "storage/secret/token/123/456/my-source-1/my-sink-3"
  004 ➡️  GET "storage/secret/token/123/456/my-source-2/my-sink-4"
  005 ➡️  GET "storage/secret/token/123/456/my-source-2/my-sink-5"
✔️  TXN | succeeded: true

// RotateAllIn - MaxUsedDiskSizeBySliceIn
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["storage/stats/staging/123/456/my-source-1/my-sink-1/", "storage/stats/staging/123/456/my-source-1/my-sink-10")
  002 ➡️  GET ["storage/stats/target/123/456/my-source-1/my-sink-1/", "storage/stats/target/123/456/my-source-1/my-sink-10")
  003 ➡️  GET ["storage/stats/staging/123/456/my-source-1/my-sink-2/", "storage/stats/staging/123/456/my-source-1/my-sink-20")
  004 ➡️  GET ["storage/stats/target/123/456/my-source-1/my-sink-2/", "storage/stats/target/123/456/my-source-1/my-sink-20")
  005 ➡️  GET ["storage/stats/staging/123/456/my-source-1/my-sink-3/", "storage/stats/staging/123/456/my-source-1/my-sink-30")
  006 ➡️  GET ["storage/stats/target/123/456/my-source-1/my-sink-3/", "storage/stats/target/123/456/my-source-1/my-sink-30")
  007 ➡️  GET ["storage/stats/staging/123/456/my-source-2/my-sink-4/", "storage/stats/staging/123/456/my-source-2/my-sink-40")
  008 ➡️  GET ["storage/stats/target/123/456/my-source-2/my-sink-4/", "storage/stats/target/123/456/my-source-2/my-sink-40")
  009 ➡️  GET ["storage/stats/staging/123/456/my-source-2/my-sink-5/", "storage/stats/staging/123/456/my-source-2/my-sink-50")
  010 ➡️  GET ["storage/stats/target/123/456/my-source-2/my-sink-5/", "storage/stats/target/123/456/my-source-2/my-sink-50")
✔️  TXN | succeeded: true

// RotateAllIn - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  // Sinks
  001 ["definition/sink/active/123/456/", "definition/sink/active/123/4560") MOD GREATER 0
  002 ["definition/sink/active/123/456/", "definition/sink/active/123/4560") MOD LESS %d
  003 "definition/sink/active/123/456/my-source-1/my-sink-1" MOD GREATER 0
  004 "definition/sink/active/123/456/my-source-1/my-sink-2" MOD GREATER 0
  005 "definition/sink/active/123/456/my-source-1/my-sink-3" MOD GREATER 0
  006 "definition/sink/active/123/456/my-source-2/my-sink-4" MOD GREATER 0
  007 "definition/sink/active/123/456/my-source-2/my-sink-5" MOD GREATER 0
  // Volumes
  008 ["storage/volume/writer/", "storage/volume/writer0") MOD GREATER 0
  009 ["storage/volume/writer/", "storage/volume/writer0") MOD LESS %d
  010 "storage/volume/writer/my-volume-1" MOD GREATER 0
  011 "storage/volume/writer/my-volume-2" MOD GREATER 0
  012 "storage/volume/writer/my-volume-3" MOD GREATER 0
  013 "storage/volume/writer/my-volume-4" MOD GREATER 0
  014 "storage/volume/writer/my-volume-5" MOD GREATER 0
  // Local files
  015 ["storage/file/level/local/123/456/", "storage/file/level/local/123/4560") MOD EQUAL 0
  // Local slices
  016 ["storage/slice/level/local/123/456/", "storage/slice/level/local/123/4560") MOD EQUAL 0
  // New objects must not exists
  017 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  018 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  019 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  020 "storage/file/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  021 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  022 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  023 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  024 "storage/file/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  025 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  026 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  027 "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  028 "storage/slice/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  029 "storage/file/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  030 "storage/slice/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  031 "storage/file/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  ➡️  THEN:
  // Save closed and opened files and slices - in two copies, in "all" and <level> prefix
  001 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z"
  002 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z"
  003 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z"
  004 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z"
  005 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z"
  006 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z"
  007 ➡️  PUT "storage/file/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z"
  008 ➡️  PUT "storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z"
  009 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z"
  010 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z"
  011 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z"
  012 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z"
  013 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z"
  014 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z"
  015 ➡️  PUT "storage/file/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z"
  016 ➡️  PUT "storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z"
  017 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z"
  018 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z"
  019 ➡️  PUT "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z"
  020 ➡️  PUT "storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z"
  021 ➡️  PUT "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z"
  022 ➡️  PUT "storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z"
  023 ➡️  PUT "storage/slice/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z"
  024 ➡️  PUT "storage/slice/level/local/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z"
  025 ➡️  PUT "storage/file/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z"
  026 ➡️  PUT "storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z"
  027 ➡️  PUT "storage/slice/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z"
  028 ➡️  PUT "storage/slice/level/local/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z"
  029 ➡️  PUT "storage/file/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z"
  030 ➡️  PUT "storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z"
  // IF conditions in the ELSE branch are used to detect a specific cause of failure, for example, "file already exists"
  ➡️  ELSE:
  001 ➡️  TXN
  001   ➡️  IF:
  001   001 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   004 "storage/file/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   005 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   006 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   007 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   008 "storage/file/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   009 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   010 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   011 "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   012 "storage/slice/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   013 "storage/file/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   014 "storage/slice/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   015 "storage/file/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   ➡️  ELSE:
  001   001 ➡️  TXN
  001   001   ➡️  IF:
  001   001   001 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   002 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   003 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   004 "storage/file/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   ➡️  ELSE:
  001   001   001 ➡️  TXN
  001   001   001   ➡️  IF:
  001   001   001   001 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   002 ➡️  TXN
  001   001   002   ➡️  IF:
  001   001   002   001 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   003 ➡️  TXN
  001   001   003   ➡️  IF:
  001   001   003   001 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   001   004 ➡️  TXN
  001   001   004   ➡️  IF:
  001   001   004   001 "storage/file/all/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002 ➡️  TXN
  001   002   ➡️  IF:
  001   002   001 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   002 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   003 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   004 "storage/file/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   ➡️  ELSE:
  001   002   001 ➡️  TXN
  001   002   001   ➡️  IF:
  001   002   001   001 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   002 ➡️  TXN
  001   002   002   ➡️  IF:
  001   002   002   001 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   003 ➡️  TXN
  001   002   003   ➡️  IF:
  001   002   003   001 "storage/slice/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   002   004 ➡️  TXN
  001   002   004   ➡️  IF:
  001   002   004   001 "storage/file/all/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003 ➡️  TXN
  001   003   ➡️  IF:
  001   003   001 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003   002 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003   003 "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003   ➡️  ELSE:
  001   003   001 ➡️  TXN
  001   003   001   ➡️  IF:
  001   003   001   001 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003   002 ➡️  TXN
  001   003   002   ➡️  IF:
  001   003   002   001 "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   003   003 ➡️  TXN
  001   003   003   ➡️  IF:
  001   003   003   001 "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   004 ➡️  TXN
  001   004   ➡️  IF:
  001   004   001 "storage/slice/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   004   002 "storage/file/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   004   ➡️  ELSE:
  001   004   001 ➡️  TXN
  001   004   001   ➡️  IF:
  001   004   001   001 "storage/slice/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   004   002 ➡️  TXN
  001   004   002   ➡️  IF:
  001   004   002   001 "storage/file/all/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   005 ➡️  TXN
  001   005   ➡️  IF:
  001   005   001 "storage/slice/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   005   002 "storage/file/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   005   ➡️  ELSE:
  001   005   001 ➡️  TXN
  001   005   001   ➡️  IF:
  001   005   001   001 "storage/slice/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0
  001   005   002 ➡️  TXN
  001   005   002   ➡️  IF:
  001   005   002   001 "storage/file/all/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z" MOD EQUAL 0

✔️  TXN | succeeded: true
`, rotateAllInEtcdLogs)

	// Check etcd state
	//   - Only the last file per the Sink is in the storage.FileWriting state.
	//   - Other files per the Sink are in the storage.FileClosing state.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-3/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-4/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T05:00:00.000Z/my-volume-2/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T05:00:00.000Z/my-volume-4/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T05:00:00.000Z/my-volume-5/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-3/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T04:00:00.000Z/my-volume-3/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T04:00:00.000Z/my-volume-5/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T05:00:00.000Z/my-volume-1/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T05:00:00.000Z/my-volume-3/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-2/2000-01-01T05:00:00.000Z/my-volume-5/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T03:00:00.000Z/my-volume-4/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T04:00:00.000Z/my-volume-4/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T05:00:00.000Z/my-volume-2/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T05:00:00.000Z/my-volume-4/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-2/my-sink-4/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-2/my-sink-4/2000-01-01T04:00:00.000Z/my-volume-4/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-2/my-sink-4/2000-01-01T05:00:00.000Z/my-volume-2/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-2/my-sink-5/2000-01-01T03:00:00.000Z/my-volume-5/2000-01-01T03:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-2/my-sink-5/2000-01-01T04:00:00.000Z/my-volume-3/2000-01-01T04:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source-2/my-sink-5/2000-01-01T05:00:00.000Z/my-volume-5/2000-01-01T05:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all|storage/secret/token/|storage/volume/"))
}
