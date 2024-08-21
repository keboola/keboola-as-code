package cleanup_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/secret/|storage/volume/|storage/file/all/|storage/slice/all/|runtime/`)

	// Get services
	d, mocked := dependencies.NewMockedCoordinatorScope(t, commonDeps.WithClock(clk))

	logger := mocked.DebugLogger()
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()

	// Setup cleanup interval
	cleanupInterval := 12 * time.Hour
	importedFileExpiration := time.Hour    // the first call of the doCleanup triggers it
	activeFileExpiration := 30 * time.Hour // the third call of the doCleanup triggers it
	cfg := cleanup.NewConfig()
	cfg.Interval = cleanupInterval
	cfg.ActiveFileExpiration = activeFileExpiration
	cfg.ArchivedFileExpiration = importedFileExpiration
	require.NoError(t, cleanup.NewNode(cfg, d))

	// Prepare doCleanup helper
	// -----------------------------------------------------------------------------------------------------------------
	var doCleanup func()
	{
		startTime := clk.Now()
		cleanupAttempt := 0
		doCleanup = func() {
			cleanupAttempt++
			clk.Set(startTime.Add(time.Duration(cleanupAttempt) * cleanupInterval))
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				logger.AssertJSONMessages(c, `{"level":"info","message":"deleted \"%d\" files"}`)
			}, 2*time.Second, 100*time.Millisecond)
		}
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, file
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Create the second file
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())
	}

	// Switch file1 to the FileImported state, file2 is kept in FileWriting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		files, err := fileRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, files, 2)

		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 2)
		file1 := files[0]
		slice1 := slices[0]
		require.Equal(t, file1.FileKey, slice1.FileKey)

		clk.Add(time.Hour)
		require.Equal(t, model.SliceClosing, slice1.State)
		slice1, err = sliceRepo.SwitchToUploading(slice1.SliceKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Equal(t, model.SliceUploading, slice1.State)
		slice1, err = sliceRepo.SwitchToUploaded(slice1.SliceKey, clk.Now(), false).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Equal(t, model.SliceUploaded, slice1.State)

		clk.Add(time.Hour)
		require.Equal(t, model.FileClosing, file1.State)
		file1, err = fileRepo.SwitchToImporting(file1.FileKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Equal(t, model.FileImporting, file1.State)
		file1, err = fileRepo.SwitchToImported(file1.FileKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Equal(t, model.FileImported, file1.State)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, []string{
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
			"storage/file/level/target/123/456/my-source/my-sink/2000-01-01T00:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z",
			"storage/slice/level/target/123/456/my-source/my-sink/2000-01-01T00:00:00.000Z/my-volume-1/2000-01-01T00:00:00.000Z",
		}, ignoredEtcdKeys)
	}

	// Delete imported file, they have shorten expiration
	// -----------------------------------------------------------------------------------------------------------------
	{
		logger.Truncate()
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of expired files"}
{"level":"info","message":"deleted expired file","file.state":"imported","file.age":"9h0m0s","file.key":"123/456/my-source/my-sink/2000-01-01T00:00:00.000Z"}
{"level":"info","message":"deleted \"1\" files","deletedFilesCount":1}
`)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, []string{
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z",
		}, ignoredEtcdKeys)
	}

	// Nothing to delete
	// -----------------------------------------------------------------------------------------------------------------
	{
		logger.Truncate()
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of expired files"}
{"level":"info","message":"deleted \"0\" files","deletedFilesCount":0}
`)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, []string{
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z",
		}, ignoredEtcdKeys)
	}

	// Delete active file, they have longer expiration
	// -----------------------------------------------------------------------------------------------------------------
	{
		logger.Truncate()
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of expired files"}
{"level":"info","message":"deleted expired file","file.state":"writing","file.age":"35h0m0s","file.key":"123/456/my-source/my-sink/2000-01-01T01:00:00.000Z"}
{"level":"info","message":"deleted \"1\" files","deletedFilesCount":1}
`)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, nil, ignoredEtcdKeys)
	}
}
