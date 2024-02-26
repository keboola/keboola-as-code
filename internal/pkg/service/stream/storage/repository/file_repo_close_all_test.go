package repository_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestFileRepository_CloseAllIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, commonDeps.WithClock(clk))
	rb := rollback.New(d.Logger())
	client := mocked.TestEtcdClient()
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
	branch := test.NewBranch(branchKey)
	require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
	source := test.NewSource(sourceKey)
	require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
	sink := test.NewSink(sinkKey)
	require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
	require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())

	// Create 2 files, with 2 slices
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).Err())
	clk.Add(time.Hour)
	require.NoError(t, fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).Err())

	// Close the last file
	// -----------------------------------------------------------------------------------------------------------------
	clk.Add(time.Hour)
	etcdLogs.Reset()
	require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
	closeAllInEtcdLogs := etcdLogs.String()

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// CloseAllIn - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  // Sink
  001 ➡️  GET "definition/sink/active/123/456/my-source/my-sink-1"
  // Local files
  002 ➡️  GET ["storage/file/level/local/123/456/my-source/my-sink-1/", "storage/file/level/local/123/456/my-source/my-sink-10")
  // Local slices
  003 ➡️  GET ["storage/slice/level/local/123/456/my-source/my-sink-1/", "storage/slice/level/local/123/456/my-source/my-sink-10")
✔️  TXN | succeeded: true

// CloseAllIn - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  // Sink
  001 "definition/sink/active/123/456/my-source/my-sink-1" MOD GREATER 0
  002 "definition/sink/active/123/456/my-source/my-sink-1" MOD LESS %d
  // Local files
  003 ["storage/file/level/local/123/456/my-source/my-sink-1/", "storage/file/level/local/123/456/my-source/my-sink-10") MOD GREATER 0
  004 ["storage/file/level/local/123/456/my-source/my-sink-1/", "storage/file/level/local/123/456/my-source/my-sink-10") MOD LESS %d
  005 "storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  006 "storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z" MOD GREATER 0
  // Local slices
  007 ["storage/slice/level/local/123/456/my-source/my-sink-1/", "storage/slice/level/local/123/456/my-source/my-sink-10") MOD GREATER 0
  008 ["storage/slice/level/local/123/456/my-source/my-sink-1/", "storage/slice/level/local/123/456/my-source/my-sink-10") MOD LESS %d
  009 "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  010 "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z" MOD GREATER 0
  ➡️  THEN:
  // Save closed file - in two copies, in "all" and <level> prefix
  001 ➡️  PUT "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z"
  002 ➡️  PUT "storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z"
  // Save closed slice - in two copies, in "all" and <level> prefix
  003 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z"
  004 ➡️  PUT "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z"
✔️  TXN | succeeded: true
`, closeAllInEtcdLogs)

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	expectedEtcdState := `
<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  %A
  "state": "closing",
  "closingAt": "2000-01-01T02:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  %A
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T01:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T02:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>
`
	etcdhelper.AssertKVsString(t, client, expectedEtcdState, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))

	// Call CloseAllIn again - no change
	clk.Add(time.Hour)
	require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, expectedEtcdState, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))
}
