package collector_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCollector(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	cfg := statistics.NewConfig().Collector

	ignoredEtcdKeys := etcdhelper.WithIgnoredKeyPattern("^(definition/)|(storage/volume/)|(storage/file/)|(storage/slice/)")

	d, mock := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()

	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()
	sliceRepo := storageRepo.Slice()

	writerEvents := &testEvents{}

	syncCounter := 0
	triggerSyncAndWait := func() {
		syncCounter++
		clk.Advance(cfg.SyncInterval.Duration())
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, syncCounter, strings.Count(mock.DebugLogger().AllMessages(), "sync done"))
		}, time.Second, 10*time.Millisecond)
	}

	// Register active volumes
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 2)
	}

	// Create branch, source, sink, file, slice
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), test.ByUser(), "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), test.ByUser(), "Create sink").Do(ctx).Err())
	}

	// Get slices
	slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 2)
	slice1 := slices[0]
	slice2 := slices[1]

	// Start collector
	collector.Start(d, writerEvents, cfg, "test-node")

	// The collector should listen on writers events
	require.NotNil(t, writerEvents.WriterOpen)
	require.NotNil(t, writerEvents.WriterClose)

	// Create 2 writers
	w1 := &testWriter{SliceKeyValue: slice1.SliceKey}
	w2 := &testWriter{SliceKeyValue: slice2.SliceKey}
	require.NoError(t, writerEvents.WriterOpen(w1))
	require.NoError(t, writerEvents.WriterOpen(w2))

	// Sync: no data
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_001.txt", ignoredEtcdKeys)

	// Sync: one writer
	w1.RowsCountValue = 1
	w1.FirstRowAtValue = utctime.MustParse("2000-01-01T01:10:00.000Z")
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:10:00.000Z")
	w1.CompressedSizeValue = 10
	w1.UncompressedSizeValue = 100
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_002.txt", ignoredEtcdKeys)

	// Sync: no change
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_002.txt", ignoredEtcdKeys)

	// sync: two writers
	w1.RowsCountValue = 5
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:30:00.000Z")
	w1.CompressedSizeValue = 50
	w1.UncompressedSizeValue = 500
	w2.RowsCountValue = 1
	w2.FirstRowAtValue = utctime.MustParse("2000-01-01T01:25:00.000Z")
	w2.LastRowAtValue = utctime.MustParse("2000-01-01T01:25:00.000Z")
	w2.CompressedSizeValue = 10
	w2.UncompressedSizeValue = 100
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_003.txt", ignoredEtcdKeys)

	// Close the writer 1
	w1.RowsCountValue = 6
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:40:00.000Z")
	w1.CompressedSizeValue = 60
	w1.UncompressedSizeValue = 600
	require.NoError(t, writerEvents.WriterClose(w1, nil))
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_004.txt", ignoredEtcdKeys)

	// Shutdown: stop Collector and remaining writer 2
	w2.RowsCountValue = 3
	w2.LastRowAtValue = utctime.MustParse("2000-01-01T01:35:00.000Z")
	w2.CompressedSizeValue = 30
	w2.UncompressedSizeValue = 300
	d.Process().Shutdown(context.Background(), errors.New("bye bye"))
	d.Process().WaitForShutdown()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_005.txt", ignoredEtcdKeys)
}

type testEvents struct {
	WriterOpen  func(w encoding.Pipeline) error
	WriterClose func(w encoding.Pipeline, closeErr error) error
}

func (e *testEvents) OnOpen(fn func(encoding.Pipeline) error) {
	e.WriterOpen = fn
}

func (e *testEvents) OnClose(fn func(encoding.Pipeline, error) error) {
	e.WriterClose = fn
}

// testWriter implements writer.Writer interface.
type testWriter struct {
	SliceKeyValue         model.SliceKey
	InProgressWritesValue uint64
	RowsCountValue        uint64
	FirstRowAtValue       utctime.UTCTime
	LastRowAtValue        utctime.UTCTime
	CompressedSizeValue   datasize.ByteSize
	UncompressedSizeValue datasize.ByteSize
}

func (w *testWriter) SliceKey() model.SliceKey {
	return w.SliceKeyValue
}

func (w *testWriter) AcceptedWrites() uint64 {
	return w.InProgressWritesValue
}

func (w *testWriter) CompletedWrites() uint64 {
	return w.RowsCountValue
}

func (w *testWriter) FirstRecordAt() utctime.UTCTime {
	return w.FirstRowAtValue
}

func (w *testWriter) LastRecordAt() utctime.UTCTime {
	return w.LastRowAtValue
}

func (w *testWriter) CompressedSize() datasize.ByteSize {
	return w.CompressedSizeValue
}

func (w *testWriter) UncompressedSize() datasize.ByteSize {
	return w.UncompressedSizeValue
}

func (w *testWriter) WriteRecord(recordctx.Context) (int, error) {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) IsReady() bool {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) Sync(context.Context) error {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) Flush(context.Context) error {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) Close(context.Context) error {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) Events() *events.Events[encoding.Pipeline] {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) DirPath() string {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) FilePath() string {
	panic(errors.New("method should not be called"))
}
