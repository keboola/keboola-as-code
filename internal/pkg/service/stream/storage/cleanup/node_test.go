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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/secret/|storage/volume/|storage/file/all/|storage/slice/all/|runtime/`)

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))

	logger := mocked.DebugLogger()
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Setup cleanup interval
	cleanupInterval := 12 * time.Hour
	activeFileExpiration := 24 * time.Hour
	importedFileExpiration := time.Hour
	cfg := cleanup.NewConfig()
	cfg.Interval = cleanupInterval
	cfg.ActiveFileExpiration = activeFileExpiration
	cfg.ArchivedFileExpiration = importedFileExpiration
	require.NoError(t, cleanup.NewNode(cfg, d))

	// The cleanup is triggered by a timer
	startTime := clk.Now()
	cleanupAttempt := 0
	doCleanup := func() {
		cleanupAttempt++
		clk.Set(startTime.Add(time.Duration(cleanupAttempt) * cleanupInterval))
		assert.Eventually(t, func() bool {
			return logger.CompareJSONMessages(`{"level":"info","message":"deleted \"%d\" files"}`) == nil
		}, 2*time.Second, 100*time.Millisecond)
	}

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	test.MockCreateFilesStorageAPICalls(t, clk, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 2)
	}

	// Create parent branch, source, sink
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(clk.Now(), &branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(clk.Now(), "Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		sink.Config = sink.Config.With(testconfig.LocalVolumeConfig(2, []string{"default"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create 5 files
	// -----------------------------------------------------------------------------------------------------------------
	var files []model.FileKey
	slices := make(map[model.FileKey][]model.SliceKey)
	for i := 0; i < 4; i++ {
		clk.Add(time.Hour)
		file, err := fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		files = append(files, file.FileKey)
		fileSlices, err := sliceRepo.ListIn(file.FileKey).Do(ctx).All()
		require.Len(t, fileSlices, 2)
		require.NoError(t, err)
		for _, slice := range fileSlices {
			slices[file.FileKey] = append(slices[file.FileKey], slice.SliceKey)
		}
	}
	etcdhelper.AssertKeys(t, client, []string{
		"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
		"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z",
		"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z",
		"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z",
		"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z",
	}, ignoredEtcdKeys)

	// Switch files and slices to different states
	// -----------------------------------------------------------------------------------------------------------------
	clk.Add(time.Minute)
	{
		// File 4 is in the model.FileWriting state
		file, err := fileRepo.Get(files[3]).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileWriting, file.State)
	}
	{
		// File 3 is in the model.FileWriting state
		file, err := fileRepo.Get(files[2]).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileClosing, file.State)
	}
	{
		// Switch file 2 to the model.FileImporting state
		fileKey := files[1]
		for _, sliceKey := range slices[fileKey] {
			test.SwitchSliceStates(t, ctx, clk, sliceRepo, sliceKey, time.Minute, []model.SliceState{
				model.SliceClosing, model.SliceUploading, model.SliceUploaded,
			})
		}
		test.SwitchFileStates(t, ctx, clk, fileRepo, fileKey, time.Minute, []model.FileState{
			model.FileClosing, model.FileImporting,
		})
		file, err := fileRepo.Get(fileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileImporting, file.State)
	}
	{
		// Switch file 1 to the model.FileImported state
		fileKey := files[0]
		for _, sliceKey := range slices[fileKey] {
			test.SwitchSliceStates(t, ctx, clk, sliceRepo, sliceKey, time.Minute, []model.SliceState{
				model.SliceClosing, model.SliceUploading, model.SliceUploaded,
			})
		}
		test.SwitchFileStates(t, ctx, clk, fileRepo, fileKey, time.Minute, []model.FileState{
			model.FileClosing, model.FileImporting, model.FileImported,
		})
		file, err := fileRepo.Get(fileKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, model.FileImported, file.State)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, []string{
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z",
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z",
			"storage/file/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z",
			"storage/file/level/target/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z",
			"storage/slice/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z",
			"storage/slice/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z",
			"storage/slice/level/target/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z",
			"storage/slice/level/target/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z",
		}, ignoredEtcdKeys)
	}

	// Delete import files, they have shorten expiration
	// -----------------------------------------------------------------------------------------------------------------
	{
		logger.Truncate()
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of expired files"}
{"level":"info","message":"deleted expired file","file.state":"imported","file.age":"7h48m0s","file.key":"123/456/my-source/my-sink/2000-01-01T01:00:00.000Z"}
{"level":"info","message":"deleted \"1\" files","deletedFilesCount":1}
`)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, []string{
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z",
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z",
			"storage/file/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z",
			"storage/slice/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z",
			"storage/slice/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z",
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
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z",
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z",
			"storage/file/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z",
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z",
			"storage/slice/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z",
			"storage/slice/level/staging/123/456/my-source/my-sink/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z",
		}, ignoredEtcdKeys)
	}

	// Delete active files, they have longer expiration
	// Note: files are deleted in parallel, so log records have random order
	// -----------------------------------------------------------------------------------------------------------------
	{
		logger.Truncate()
		doCleanup()
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of expired files"}
{"level":"info","message":"deleted \"3\" files","deletedFilesCount":3,"component":"storage.cleanup"}
`)
		logger.AssertJSONMessages(t, `{"level":"info","message":"deleted expired file","file.state":"importing","file.age":"31h54m0s","file.key":"123/456/my-source/my-sink/2000-01-01T02:00:00.000Z"}`)
		logger.AssertJSONMessages(t, `{"level":"info","message":"deleted expired file","file.state":"closing","file.age":"32h0m0s","file.key":"123/456/my-source/my-sink/2000-01-01T03:00:00.000Z"}`)
		logger.AssertJSONMessages(t, `{"level":"info","message":"deleted expired file","file.state":"writing","file.age":"32h0m0s","file.key":"123/456/my-source/my-sink/2000-01-01T04:00:00.000Z"}`)
	}
	{
		// Check database state
		etcdhelper.AssertKeys(t, client, nil, ignoredEtcdKeys)
	}
}
