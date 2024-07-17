package collector_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCollector(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	cfg := statistics.NewConfig().Collector

	d, mock := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
	writerEvents := &testEvents{}

	syncCounter := 0
	triggerSyncAndWait := func() {
		syncCounter++
		clk.Add(cfg.SyncInterval.Duration())
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, syncCounter, strings.Count(mock.DebugLogger().AllMessages(), "sync done"))
		}, time.Second, 10*time.Millisecond)
	}

	// Start collector
	collector.Start(d, writerEvents, cfg, "test-node")

	// The collector should listen on writers events
	require.NotNil(t, writerEvents.WriterOpen)
	require.NotNil(t, writerEvents.WriterClose)

	// Create 3 writers
	w1 := &testWriter{SliceKeyValue: test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")}
	w2 := &testWriter{SliceKeyValue: test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")}
	assert.NoError(t, writerEvents.WriterOpen(w1))
	assert.NoError(t, writerEvents.WriterOpen(w2))

	// Sync: no data
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_001.txt")

	// Sync: one writer
	w1.RowsCountValue = 1
	w1.FirstRowAtValue = utctime.MustParse("2000-01-01T01:10:00.000Z")
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:10:00.000Z")
	w1.CompressedSizeValue = 10
	w1.UncompressedSizeValue = 100
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_002.txt")

	// Sync: no change
	triggerSyncAndWait()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_002.txt")

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
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_003.txt")

	// Close the writer 1
	w1.RowsCountValue = 6
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:40:00.000Z")
	w1.CompressedSizeValue = 60
	w1.UncompressedSizeValue = 600
	assert.NoError(t, writerEvents.WriterClose(w1, nil))
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_004.txt")

	// Shutdown: stop Collector and remaining writer 2
	w2.RowsCountValue = 3
	w2.LastRowAtValue = utctime.MustParse("2000-01-01T01:35:00.000Z")
	w2.CompressedSizeValue = 30
	w2.UncompressedSizeValue = 300
	d.Process().Shutdown(context.Background(), errors.New("bye bye"))
	d.Process().WaitForShutdown()
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_collector_snapshot_005.txt")
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

func (w *testWriter) WriteRecord(recordctx.Context) error {
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
