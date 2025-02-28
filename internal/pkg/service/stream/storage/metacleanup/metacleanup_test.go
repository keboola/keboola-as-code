package metacleanup_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	keboolaSink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	keboolaModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	bridgeTest "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/metacleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	bridgeEntity "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/bridge"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type (
	// token is an etcd prefix that stores all Keboola Storage API token entities.
	testToken struct {
		etcdop.PrefixT[keboolaSink.Token]
	}
)

func forToken(s *serde.Serde) testToken {
	return testToken{PrefixT: etcdop.NewTypedPrefix[keboolaSink.Token]("storage/keboola/secret/token", s)}
}

func (v testToken) ForSink(k key.SinkKey) etcdop.KeyT[keboolaSink.Token] {
	return v.PrefixT.Key(k.String())
}

func TestMetadataCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	jobKey := keboolaModel.JobKey{SinkKey: sinkKey, JobID: "321"}
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/secret/|storage/volume/|storage/file/all/|storage/slice/all/|storage/stats/|runtime/|storage/keboola/secret/token/`)

	// Get services
	d, mocked := dependencies.NewMockedCoordinatorScope(t, ctx, commonDeps.WithClock(clk))

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
	cfg := metacleanup.NewConfig()
	cfg.Interval = cleanupInterval
	cfg.ActiveFileExpiration = activeFileExpiration
	cfg.ArchivedFileExpiration = importedFileExpiration

	// Register routes for receiving information about jobs
	transport := mocked.MockedHTTPTransport()
	{
		bridgeTest.MockSuccessJobStorageAPICalls(t, transport)
	}

	// Start metadata cleanup node
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, metacleanup.Start(d, cfg))

	// Prepare doCleanup helper
	// -----------------------------------------------------------------------------------------------------------------
	var doCleanup func()
	{
		startTime := clk.Now()
		cleanupAttempt := 0
		doCleanup = func() {
			cleanupAttempt++
			clk.Advance(startTime.Add(time.Duration(cleanupAttempt) * cleanupInterval).Sub(clk.Now()))
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

	// Create parent branch, source, sink, token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		token := keboolaSink.Token{SinkKey: sinkKey, Token: &keboola.Token{ID: "secret"}}
		require.NoError(t, forToken(d.EtcdSerde()).ForSink(sinkKey).Put(client, token).Do(ctx).Err())
	}

	// Create the second file
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Advance(time.Hour)
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

		clk.Advance(time.Hour)
		require.Equal(t, model.SliceClosing, slice1.State)
		slice1, err = sliceRepo.SwitchToUploading(slice1.SliceKey, clk.Now(), false).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Equal(t, model.SliceUploading, slice1.State)
		slice1, err = sliceRepo.SwitchToUploaded(slice1.SliceKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Equal(t, model.SliceUploaded, slice1.State)

		clk.Advance(time.Hour)
		require.Equal(t, model.FileClosing, file1.State)
		file1, err = fileRepo.SwitchToImporting(file1.FileKey, clk.Now(), false).Do(ctx).ResultOrErr()
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
{"level":"info","message":"deleted expired file","file.state":"imported","file.age":"9h0m0s","file.id":"2000-01-01T00:00:00.000Z"}
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
{"level":"info","message":"deleted expired file","file.state":"writing","file.age":"35h0m0s","file.id":"2000-01-01T01:00:00.000Z"}
{"level":"info","message":"deleted \"1\" files","deletedFilesCount":1}
`)
	}
	// Check database state
	{
		etcdhelper.AssertKeys(t, client, nil, ignoredEtcdKeys)
	}

	// Create job in global level and bridge level
	{
		job := bridgeEntity.NewJob(jobKey)
		require.NoError(t, d.KeboolaBridgeRepository().Job().Create(&job).Do(ctx).Err())
	}

	// Delete success job as it has finished
	jobCleanupAttempt := 0
	{
		logger.Truncate()
		jobCleanupAttempt++
		clk.Advance(time.Duration(jobCleanupAttempt) * cleanupInterval)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, `{"level":"info","message":"deleted \"%d\" jobs"}`)
		}, 2*time.Second, 100*time.Millisecond)
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of success jobs"}
{"level":"info","message":"deleted finished storage job"}
{"level":"info","message":"deleted \"1\" jobs","deletedJobsCount":1}
`)
	}
	// Check database state
	{
		etcdhelper.AssertKeys(t, client, nil, ignoredEtcdKeys)
	}
}

func TestMetadataProcessingJobCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	jobKey := keboolaModel.JobKey{SinkKey: sinkKey, JobID: "321"}
	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/secret/|storage/volume/|storage/file/all/|storage/file/level/local/|storage/slice/all/|storage/slice/level/local/|storage/stats/|runtime/|storage/keboola/secret/token/`)

	// Get services
	d, mocked := dependencies.NewMockedCoordinatorScope(t, ctx, commonDeps.WithClock(clk))

	logger := mocked.DebugLogger()
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()

	// Setup cleanup interval
	cleanupInterval := 12 * time.Hour
	importedFileExpiration := time.Hour    // the first call of the doCleanup triggers it
	activeFileExpiration := 30 * time.Hour // the third call of the doCleanup triggers it
	cfg := metacleanup.NewConfig()
	cfg.Interval = cleanupInterval
	cfg.ActiveFileExpiration = activeFileExpiration
	cfg.ArchivedFileExpiration = importedFileExpiration

	// Register routes for receiving information about jobs
	transport := mocked.MockedHTTPTransport()
	{
		bridgeTest.MockProcessingJobStorageAPICalls(t, transport)
	}

	// Start metadata cleanup node
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, metacleanup.Start(d, cfg))

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		token := keboolaSink.Token{SinkKey: sinkKey, Token: &keboola.Token{ID: "secret"}}
		require.NoError(t, forToken(d.EtcdSerde()).ForSink(sinkKey).Put(client, token).Do(ctx).Err())
	}

	// Create job in global level and bridge level
	{
		job := bridgeEntity.NewJob(jobKey)
		require.NoError(t, d.KeboolaBridgeRepository().Job().Create(&job).Do(ctx).Err())
	}

	// Delete processing job as it has finished
	jobCleanupAttempt := 0
	{
		logger.Truncate()
		jobCleanupAttempt++
		clk.Advance(time.Duration(jobCleanupAttempt) * cleanupInterval)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, `{"level":"info","message":"deleted \"%d\" jobs"}`)
		}, 2*time.Second, 100*time.Millisecond)
		logger.AssertJSONMessages(t, `
{"level":"info","message":"deleting metadata of success jobs"}
{"level":"debug","message":"cannot remove storage job, job status: processing"}
{"level":"info","message":"deleted \"0\" jobs","deletedJobsCount":0}
`)
	}
	// Check database state
	{
		etcdhelper.AssertKeys(
			t,
			client,
			[]string{
				"storage/keboola/job/123/456/my-source/my-sink/321",
			},
			ignoredEtcdKeys,
		)
	}
}

func TestMetadataProcessingJobCleanupErrorTolerance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Get services
	d, mocked := dependencies.NewMockedCoordinatorScope(t, ctx, commonDeps.WithClock(clk))

	logger := mocked.DebugLogger()
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()

	// Setup cleanup interval
	cleanupInterval := 12 * time.Hour
	importedFileExpiration := time.Hour    // the first call of the doCleanup triggers it
	activeFileExpiration := 30 * time.Hour // the third call of the doCleanup triggers it
	cfg := metacleanup.NewConfig()
	cfg.Concurrency = 2
	cfg.ErrorTolerance = 3
	cfg.Interval = cleanupInterval
	cfg.ActiveFileExpiration = activeFileExpiration
	cfg.ArchivedFileExpiration = importedFileExpiration

	// Register routes for receiving information about jobs
	transport := mocked.MockedHTTPTransport()
	{
		bridgeTest.MockProcessingJobStorageAPICalls(t, transport)
	}

	// Start metadata cleanup node
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, metacleanup.Start(d, cfg))

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		token := keboolaSink.Token{SinkKey: sinkKey, Token: &keboola.Token{ID: "secret"}}
		require.NoError(t, forToken(d.EtcdSerde()).ForSink(sinkKey).Put(client, token).Do(ctx).Err())
	}

	// Create jobs. IDs are intentionally invalid to trigger an error in cleanup
	for i := range 20 {
		job := bridgeEntity.NewJob(keboolaModel.JobKey{SinkKey: sinkKey, JobID: keboolaModel.JobID("job" + strconv.Itoa(i))})
		require.NoError(t, d.KeboolaBridgeRepository().Job().Create(&job).Do(ctx).Err())
	}

	// One job with valid id, this one should fail with "context canceled"
	job := bridgeEntity.NewJob(keboolaModel.JobKey{SinkKey: sinkKey, JobID: keboolaModel.JobID(strconv.Itoa(1))})
	require.NoError(t, d.KeboolaBridgeRepository().Job().Create(&job).Do(ctx).Err())

	// Cleanup
	{
		logger.Truncate()
		clk.Advance(cleanupInterval)

		time.Sleep(1 * time.Second)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			messages := logger.AllMessages()
			assert.Contains(c, messages, `context canceled`)
			actual := strings.Count(messages, `"message":"cannot get keboola storage job \"123/456/my-source/my-sink/job`)
			assert.GreaterOrEqual(c, actual, 5)
			assert.LessOrEqual(c, actual, 6)
		}, 2*time.Second, 100*time.Millisecond)
	}
}
