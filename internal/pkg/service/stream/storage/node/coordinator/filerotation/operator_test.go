package filerotation_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
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

func TestFileRotation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Storage.Level.Target.Import.Trigger = importTrigger
		cfg.Storage.Level.Target.Operator.CheckInterval = duration.From(conditionsCheckInterval)
	}, commonDeps.WithClock(clk))
	logger := mock.DebugLogger()
	client := mock.TestEtcdClient()

	// File import should be triggered 1 minute after the file opening, 30 minutes before the expiration.
	fileExpirationDiff := time.Minute
	fileExpiration := importTrigger.Expiration.Duration() + fileExpirationDiff
	mock.TestDummySinkController().FileExpiration = fileExpiration

	// Start file rotation coordinator
	require.NoError(t, filerotation.Start(d, mock.TestConfig().Storage.Level.Target.Operator))

	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	defer func() { require.NoError(t, session.Close()) }()
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 1)

	// Helpers
	waitForFilesSync := func(t *testing.T) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.file.rotation"}`)
		}, 5*time.Second, 10*time.Millisecond)
	}
	waitForStatsSync := func(t *testing.T) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"stats.cache.L1"}`)
		}, 5*time.Second, 10*time.Millisecond)
	}
	triggerCheck := func(t *testing.T, expectEntityModification bool, expectedLogs string) {
		t.Helper()
		clk.Add(conditionsCheckInterval)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, expectedLogs)
		}, 5*time.Second, 10*time.Millisecond)
		if expectEntityModification {
			waitForFilesSync(t)
		}
		logger.Truncate()
	}

	// Fixtures
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
	require.NoError(t, d.DefinitionRepository().Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, d.DefinitionRepository().Source().Create(&source, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, d.DefinitionRepository().Sink().Create(&sink, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	waitForFilesSync(t)

	// Trigger check - no import trigger
	triggerCheck(t, false, `
{"level":"debug","message":"checking files import conditions","component":"storage.node.operator.file.rotation"}                                            
{"level":"debug","message":"skipping file rotation: no record","component":"storage.node.operator.file.rotation"}
`)

	// Trigger check - file expiration import trigger
	clk.Add(fileExpirationDiff)
	triggerCheck(t, true, ` 
{"level":"info","message":"rotating file for import: expiration threshold met, expiration: 2000-01-01T00:31:00.000Z, remains: 30m0s, threshold: 30m0s","component":"storage.node.operator.file.rotation"}
`)

	// Trigger check - no import trigger
	triggerCheck(t, false, `
{"level":"debug","message":"skipping file rotation: no record","component":"storage.node.operator.file.rotation"}
`)

	// Check state
	files, err := d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, model.FileClosing, files[0].State)
	require.Equal(t, model.FileWriting, files[1].State)
	slices, err := d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceClosing, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)

	// Simulate some records count over the threshold
	slice := slices[1]
	require.NoError(t, d.StatisticsRepository().Put(ctx, "source-node-1", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     importTrigger.Count/2 + 1,
			UncompressedSize: 100 * datasize.B,
			CompressedSize:   10 * datasize.B,
		},
	}))
	require.NoError(t, d.StatisticsRepository().Put(ctx, "source-node-2", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     importTrigger.Count/2 + 1,
			UncompressedSize: 100 * datasize.B,
			CompressedSize:   10 * datasize.B,
		},
	}))
	waitForStatsSync(t)

	// Trigger check - records count trigger
	triggerCheck(t, true, ` 
{"level":"info","message":"rotating file for import: count threshold met, records count: 50002, threshold: 50000","component":"storage.node.operator.file.rotation"}
`)

	// Trigger check - no import trigger
	triggerCheck(t, false, `
{"level":"debug","message":"skipping file rotation: no record","component":"storage.node.operator.file.rotation"}
`)

	// Other conditions are tested in "TestShouldImport"

	// Check state
	files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, files, 3)
	require.Equal(t, model.FileClosing, files[0].State)
	require.Equal(t, model.FileClosing, files[1].State)
	require.Equal(t, model.FileWriting, files[2].State)
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 3)
	require.Equal(t, model.SliceClosing, slices[0].State)
	require.Equal(t, model.SliceClosing, slices[1].State)
	require.Equal(t, model.SliceWriting, slices[2].State)

	// Unblock switching to the importing state
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[0].SliceKey, d.Clock().Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploading(slices[1].SliceKey, d.Clock().Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploaded(slices[0].SliceKey, d.Clock().Now()).Do(ctx).Err())
	require.NoError(t, d.StorageRepository().Slice().SwitchToUploaded(slices[1].SliceKey, d.Clock().Now()).Do(ctx).Err())
	triggerCheck(t, false, "")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		files, err = d.StorageRepository().File().ListIn(sink.SinkKey).Do(ctx).All()
		assert.NoError(c, err)
		assert.Len(c, files, 3)
		assert.Equal(c, model.FileImporting, files[0].State)
		assert.Equal(c, model.FileImporting, files[1].State)
		assert.Equal(c, model.FileWriting, files[2].State)
		slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
		assert.NoError(c, err)
		assert.Len(c, slices, 3)
		assert.Equal(c, model.SliceUploaded, slices[0].State)
		assert.Equal(c, model.SliceUploaded, slices[1].State)
		assert.Equal(c, model.SliceWriting, slices[2].State)
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// No error is logged
	logger.AssertJSONMessages(t, "")
}
