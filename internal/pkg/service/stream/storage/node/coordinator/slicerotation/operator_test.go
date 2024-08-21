package slicerotation_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/slicerotation"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestSliceRotation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	uploadTrigger := stagingConfig.UploadTrigger{
		Count:    50000,
		Size:     5 * datasize.MB,
		Interval: duration.From(5 * time.Minute),
	}

	// The interval triggers upload conditions check
	conditionsCheckInterval := time.Second

	// Create dependencies
	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())
	d, mock := dependencies.NewMockedCoordinatorScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Storage.Level.Staging.Upload.Trigger = uploadTrigger
		cfg.Storage.Level.Staging.Operator.SliceRotationCheckInterval = duration.From(conditionsCheckInterval)
	}, commonDeps.WithClock(clk))
	logger := mock.DebugLogger()
	client := mock.TestEtcdClient()

	// Block switching to the uploading state, a source is using slice
	sourceNode, err := closesync.NewSourceNode(d, "source-node")
	require.NoError(t, err)

	// Start slice rotation coordinator
	require.NoError(t, slicerotation.Start(d, mock.TestConfig().Storage.Level.Staging.Operator))

	// Register some volumes
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)
	defer func() { require.NoError(t, session.Close()) }()
	test.RegisterWriterVolumes(t, ctx, d.StorageRepository().Volume(), session, 1)

	// Helpers
	waitForFilesSync := func(t *testing.T) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.node.operator.slice.rotation"}`)
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

	// Trigger check - no upload trigger
	triggerCheck(t, false, `
{"level":"debug","message":"skipping slice rotation: no record","component":"storage.node.operator.slice.rotation"}
`)

	// Simulate some records count over the threshold
	slices, err := d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	require.Equal(t, model.SliceWriting, slices[0].State)
	slice := slices[0]
	require.NoError(t, d.StatisticsRepository().Put(ctx, "source-node-1", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     uploadTrigger.Count/2 + 1,
			UncompressedSize: 100 * datasize.B,
			CompressedSize:   10 * datasize.B,
		},
	}))
	require.NoError(t, d.StatisticsRepository().Put(ctx, "source-node-2", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     uploadTrigger.Count/2 + 1,
			UncompressedSize: 100 * datasize.B,
			CompressedSize:   10 * datasize.B,
		},
	}))
	waitForStatsSync(t)

	// Trigger check - records count trigger
	triggerCheck(t, true, ` 
{"level":"info","message":"rotating slice for upload: count threshold met, records count: 50002, threshold: 50000","component":"storage.node.operator.slice.rotation"}
`)

	// Trigger check - no upload trigger
	triggerCheck(t, false, `
{"level":"debug","message":"skipping slice rotation: no record","component":"storage.node.operator.slice.rotation"}
`)

	// Simulate some bytes over the threshold
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	require.Equal(t, model.SliceClosing, slices[0].State)
	require.Equal(t, model.SliceWriting, slices[1].State)
	slice = slices[1]
	require.NoError(t, d.StatisticsRepository().Put(ctx, "source-node-1", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   uploadTrigger.Size/2 + 1,
		},
	}))
	require.NoError(t, d.StatisticsRepository().Put(ctx, "source-node-2", []statistics.PerSlice{
		{
			SliceKey:         slice.SliceKey,
			FirstRecordAt:    slice.OpenedAt(),
			LastRecordAt:     slice.OpenedAt().Add(time.Second),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   uploadTrigger.Size/2 + 1,
		},
	}))
	waitForStatsSync(t)

	// Trigger check - compressed size trigger
	triggerCheck(t, true, `
{"level":"info","message":"rotating slice for upload: size threshold met, compressed size: 5.0 MB, threshold: 5.0 MB","slice.key":"2000-01-01T00:00:02.000Z","component":"storage.node.operator.slice.rotation"}
{"level":"info","time":"%s","message":"rotating of slice finished","slice.key":"2000-01-01T00:00:02.000Z","component":"storage.node.operator.slice.rotation"}
`)
	// Other conditions are tested in "TestShouldUpload"

	// Check slices state
	slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 3)
	require.Equal(t, model.SliceClosing, slices[0].State)
	require.Equal(t, model.SliceClosing, slices[1].State)
	require.Equal(t, model.SliceWriting, slices[2].State)

	// Unblock switching to the uploading state, the source node is updated
	resp, err := mock.TestEtcdClient().Get(ctx, "foo")
	require.NoError(t, err)
	require.NoError(t, sourceNode.Notify(ctx, resp.Header.Revision))
	triggerCheck(t, false, `
{"level":"info","time":"%s","message":"closing slice","slice.key":"2000-01-01T00:00:02.000Z","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"closing slice finished","slice.key":"2000-01-01T00:00:02.000Z","component":"storage.node.operator.slice.rotation"}
	`)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		slices, err = d.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
		assert.NoError(c, err)
		assert.Len(c, slices, 3)
		assert.Equal(c, model.SliceUploading, slices[0].State)
		assert.Equal(c, model.SliceUploading, slices[1].State)
		assert.Equal(c, model.SliceWriting, slices[2].State)
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// No error is logged
	logger.AssertJSONMessages(t, "")
}
