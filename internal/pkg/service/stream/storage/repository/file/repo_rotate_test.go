package file_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestFileRepository_Rotate(t *testing.T) {
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
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	volumeRepo := storageRepo.Volume()

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 2)
	}

	// Create parent branch, source, sink and token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now()).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), "Create source").Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		sink.Config = sink.Config.With(testconfig.LocalVolumeConfig(2, []string{"default"}))
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), "Create sink").Do(ctx).Err())
	}

	// Create (the first file Rotate operation)
	// -----------------------------------------------------------------------------------------------------------------
	var file1 model.File
	{
		var err error
		clk.Add(time.Hour)
		file1, err = fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file1.OpenedAt().Time())
	}

	// Rotate file (2)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		file2, err := fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file2.OpenedAt().Time())
	}

	// Rotate file (3)
	// -----------------------------------------------------------------------------------------------------------------
	var file3 model.File
	{
		var err error
		clk.Add(time.Hour)
		file3, err = fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file3.OpenedAt().Time())
	}

	// Check etcd state
	//   - Only the last file is in the storage.FileWriting state.
	//   - Other files are in the storage.FileClosing state.
	//   - Slices are switched to the storage.SliceClosing state together with the file state transition.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
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

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))
}
