package filerotation_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdPkg "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/filerotation"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// fileExpirationDiff defines the time between file opening and file import trigger.
const fileExpirationDiff = time.Minute

type testState struct {
	interval      time.Duration
	importTrigger targetConfig.ImportTrigger
	clk           *clock.Mock
	logger        log.DebugLogger
	client        *etcdPkg.Client
	mock          dependencies.Mocked
	dependencies  dependencies.CoordinatorScope
	session       *concurrency.Session
	sink          definition.Sink
}

func setup(t *testing.T, ctx context.Context) *testState {
	t.Helper()

	importTrigger := targetConfig.ImportTrigger{
		Count:       50000,
		Size:        5 * datasize.MB,
		Interval:    duration.From(5 * time.Minute),
		SlicesCount: 100,
		Expiration:  duration.From(30 * time.Minute),
	}

	// The interval triggers import conditions check
	conditionsCheckInterval := time.Second

	// Create dependencies
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Import.Trigger = importTrigger
		cfg.Storage.Level.Target.Operator.FileRotationCheckInterval = duration.From(conditionsCheckInterval)
	}, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()

	// File import should be triggered 1 minute after the file opening, 30 minutes before the expiration.
	fileExpiration := importTrigger.Expiration.Duration() + fileExpirationDiff
	mock.TestDummySinkController().FileExpiration = fileExpiration

	// Start file rotation coordinator
	require.NoError(t, filerotation.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 2)

	return &testState{
		interval:      conditionsCheckInterval,
		importTrigger: importTrigger,
		clk:           clk,
		mock:          mock,
		logger:        mock.DebugLogger(),
		client:        client,
		dependencies:  d,
		session:       session,
	}
}

func (ts *testState) teardown(t *testing.T) {
	t.Helper()
	require.NoError(t, ts.session.Close())
}

func (ts *testState) waitForFilesSync(t *testing.T) {
	t.Helper()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.rotation"}`)
	}, 5*time.Second, 10*time.Millisecond)
}

func (ts *testState) waitForStatsSync(t *testing.T) {
	t.Helper()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"stats.cache.L1"}`)
	}, 5*time.Second, 10*time.Millisecond)
}

func (ts *testState) triggerCheck(t *testing.T, expectEntityModification bool, expectedLogs string) {
	t.Helper()
	ts.clk.Add(ts.interval)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, expectedLogs)
	}, 5*time.Second, 10*time.Millisecond)
	if expectEntityModification {
		ts.waitForFilesSync(t)
	}
	ts.logger.Truncate()
}

func (ts *testState) prepareFixtures(t *testing.T, ctx context.Context) {
	t.Helper()
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	ts.sink = dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	ts.sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
	require.NoError(t, ts.dependencies.DefinitionRepository().Branch().Create(&branch, ts.clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.DefinitionRepository().Source().Create(&source, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, ts.dependencies.DefinitionRepository().Sink().Create(&ts.sink, ts.clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	ts.waitForFilesSync(t)
}

func TestFileRotation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts := setup(t, ctx)
	defer ts.teardown(t)
	ts.prepareFixtures(t, ctx)

	// Trigger check - no import trigger
	ts.triggerCheck(t, false, `
{"level":"debug","message":"checking files import conditions","component":"storage.node.operator.file.rotation"}                                            
{"level":"debug","message":"skipping file rotation: no record","component":"storage.node.operator.file.rotation"}
`)

	// Trigger check - file expiration import trigger
	ts.clk.Add(fileExpirationDiff)
	ts.triggerCheck(t, true, ` 
{"level":"info","message":"rotating file for import: expiration threshold met, expiration: 2000-01-01T00:31:00.000Z, remains: %s, threshold: 30m0s","component":"storage.node.operator.file.rotation"}
`)

	// Trigger check - no import trigger
	ts.triggerCheck(t, false, `
{"level":"debug","message":"skipping file rotation: no record","component":"storage.node.operator.file.rotation"}
`)

	// Check state
	files, err := ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileClosing, files[0].State)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err := ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 4)
	require.Equal(t, model.SliceClosing, slices[0].State)
	require.Equal(t, model.SliceClosing, slices[1].State)
	require.Equal(t, model.SliceWriting, slices[2].State)
	require.Equal(t, model.SliceWriting, slices[3].State)

	// Simulate some records count over the threshold
	slice := slices[2]
	require.NoError(t, ts.dependencies.StatisticsRepository().Put(ctx, "source-node-1", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     ts.importTrigger.Count/2 + 1,
			UncompressedSize: 100 * datasize.B,
			CompressedSize:   10 * datasize.B,
		},
	}))
	require.NoError(t, ts.dependencies.StatisticsRepository().Put(ctx, "source-node-2", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     ts.importTrigger.Count/2 + 1,
			UncompressedSize: 100 * datasize.B,
			CompressedSize:   10 * datasize.B,
		},
	}))
	ts.waitForStatsSync(t)

	// Trigger check - records count trigger
	ts.triggerCheck(t, true, ` 
{"level":"info","message":"rotating file for import: count threshold met, records count: 50002, threshold: 50000","component":"storage.node.operator.file.rotation"}
`)

	// Trigger check - no import trigger
	ts.triggerCheck(t, false, `
{"level":"debug","message":"skipping file rotation: no record","component":"storage.node.operator.file.rotation"}
`)

	// Other conditions are tested in "TestShouldImport"

	// Check state
	files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 3)
	require.Equal(t, model.FileClosing, files[0].State)
	require.Equal(t, model.FileClosing, files[1].State)
	require.Equal(t, model.FileWriting, files[2].State)
	slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 6)
	require.Equal(t, model.SliceClosing, slices[0].State)
	require.Equal(t, model.SliceClosing, slices[1].State)
	require.Equal(t, model.SliceClosing, slices[2].State)
	require.Equal(t, model.SliceClosing, slices[3].State)
	require.Equal(t, model.SliceWriting, slices[4].State)
	require.Equal(t, model.SliceWriting, slices[5].State)

	// Unblock switching to the importing state
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slices[0].SliceKey, ts.dependencies.Clock().Now(), false).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slices[1].SliceKey, ts.dependencies.Clock().Now(), true).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slices[2].SliceKey, ts.dependencies.Clock().Now(), true).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploading(slices[3].SliceKey, ts.dependencies.Clock().Now(), true).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slices[0].SliceKey, ts.dependencies.Clock().Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slices[1].SliceKey, ts.dependencies.Clock().Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slices[2].SliceKey, ts.dependencies.Clock().Now()).Do(ctx).Err())
	require.NoError(t, ts.dependencies.StorageRepository().Slice().SwitchToUploaded(slices[3].SliceKey, ts.dependencies.Clock().Now()).Do(ctx).Err())
	ts.triggerCheck(t, false, "")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		files, err = ts.dependencies.StorageRepository().File().ListIn(ts.sink.SinkKey).Do(ctx).All()
		assert.NoError(c, err)
		assert.Len(c, files, 3)
		assert.Equal(c, model.FileImporting, files[0].State)
		assert.False(c, files[0].StagingStorage.IsEmpty)
		assert.Equal(c, model.FileImporting, files[1].State)
		assert.True(c, files[1].StagingStorage.IsEmpty)
		assert.Equal(c, model.FileWriting, files[2].State)
		slices, err = ts.dependencies.StorageRepository().Slice().ListIn(ts.sink.SinkKey).Do(ctx).All()
		assert.NoError(c, err)
		assert.Len(c, slices, 6)
		assert.Equal(c, model.SliceUploaded, slices[0].State)
		assert.False(c, slices[0].LocalStorage.IsEmpty)
		assert.Equal(c, model.SliceUploaded, slices[1].State)
		assert.True(c, slices[1].LocalStorage.IsEmpty)
		assert.Equal(c, model.SliceUploaded, slices[2].State)
		assert.True(c, slices[2].LocalStorage.IsEmpty)
		assert.Equal(c, model.SliceUploaded, slices[3].State)
		assert.True(c, slices[3].LocalStorage.IsEmpty)
		assert.Equal(c, model.SliceWriting, slices[4].State)
		assert.Equal(c, model.SliceWriting, slices[5].State)
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown
	ts.dependencies.Process().Shutdown(ctx, errors.New("bye bye"))
	ts.dependencies.Process().WaitForShutdown()

	// No error is logged
	ts.logger.AssertJSONMessages(t, "")
}
