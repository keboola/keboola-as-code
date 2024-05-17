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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCollector(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	cfg := statistics.NewConfig().Collector

	d, mock := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk), commonDeps.WithEnabledEtcdClient())
	client := d.EtcdClient()
	repo := repository.New(d)
	events := &testEvents{}

	syncCounter := 0
	triggerSyncAndWait := func() {
		syncCounter++
		clk.Add(cfg.SyncInterval.Duration())
		assert.Eventually(t, func() bool {
			return strings.Count(mock.DebugLogger().AllMessages(), "sync done") == syncCounter
		}, time.Second, 10*time.Millisecond)
	}

	// Create collector
	col := collector.New(d.Logger(), clk, repo, events, cfg)
	require.NotNil(t, col)

	// The collector should listen on writers events
	require.NotNil(t, events.WriterOpen)
	require.NotNil(t, events.WriterClose)

	// Create 3 writers
	w1 := &testWriter{SliceKeyValue: test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")}
	w2 := &testWriter{SliceKeyValue: test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")}
	assert.NoError(t, events.WriterOpen(w1))
	assert.NoError(t, events.WriterOpen(w2))

	// Sync: no data
	triggerSyncAndWait()
	etcdhelper.AssertKVsString(t, client, ``)

	// Sync: one writer
	w1.RowsCountValue = 1
	w1.FirstRowAtValue = utctime.MustParse("2000-01-01T01:10:00.000Z")
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:10:00.000Z")
	w1.CompressedSizeValue = 10
	w1.UncompressedSizeValue = 100
	triggerSyncAndWait()
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:10:00.000Z",
  "lastRecordAt": "2000-01-01T01:10:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "100B",
  "compressedSize": "10B"
}
>>>>>
`)

	// Sync: no change
	triggerSyncAndWait()
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:10:00.000Z",
  "lastRecordAt": "2000-01-01T01:10:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "100B",
  "compressedSize": "10B"
}
>>>>>
`)

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
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:10:00.000Z",
  "lastRecordAt": "2000-01-01T01:30:00.000Z",
  "recordsCount": 5,
  "uncompressedSize": "500B",
  "compressedSize": "50B"
}
>>>>>

<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:25:00.000Z",
  "lastRecordAt": "2000-01-01T01:25:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "100B",
  "compressedSize": "10B"
}
>>>>>
`)

	// Close the writer 1
	w1.RowsCountValue = 6
	w1.LastRowAtValue = utctime.MustParse("2000-01-01T01:40:00.000Z")
	w1.CompressedSizeValue = 60
	w1.UncompressedSizeValue = 600
	assert.NoError(t, events.WriterClose(w1, nil))
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:10:00.000Z",
  "lastRecordAt": "2000-01-01T01:40:00.000Z",
  "recordsCount": 6,
  "uncompressedSize": "600B",
  "compressedSize": "60B"
}
>>>>>

<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:25:00.000Z",
  "lastRecordAt": "2000-01-01T01:25:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "100B",
  "compressedSize": "10B"
}
>>>>>
`)

	// Stop periodical sync
	col.Stop(context.Background())

	// Close writer 2
	w2.RowsCountValue = 3
	w2.LastRowAtValue = utctime.MustParse("2000-01-01T01:35:00.000Z")
	w2.CompressedSizeValue = 30
	w2.UncompressedSizeValue = 300
	assert.NoError(t, events.WriterClose(w2, nil))
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:10:00.000Z",
  "lastRecordAt": "2000-01-01T01:40:00.000Z",
  "recordsCount": 6,
  "uncompressedSize": "600B",
  "compressedSize": "60B"
}
>>>>>

<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:25:00.000Z",
  "lastRecordAt": "2000-01-01T01:35:00.000Z",
  "recordsCount": 3,
  "uncompressedSize": "300B",
  "compressedSize": "30B"
}
>>>>>
`)
}

type testEvents struct {
	WriterOpen  func(w writer.Writer) error
	WriterClose func(w writer.Writer, closeErr error) error
}

func (e *testEvents) OnWriterOpen(fn func(writer.Writer) error) {
	e.WriterOpen = fn
}

func (e *testEvents) OnWriterClose(fn func(writer.Writer, error) error) {
	e.WriterClose = fn
}

type testWriter struct {
	SliceKeyValue         model.SliceKey
	RowsCountValue        uint64
	FirstRowAtValue       utctime.UTCTime
	LastRowAtValue        utctime.UTCTime
	CompressedSizeValue   datasize.ByteSize
	UncompressedSizeValue datasize.ByteSize
}

func (w *testWriter) SliceKey() model.SliceKey {
	return w.SliceKeyValue
}

func (w *testWriter) RowsCount() uint64 {
	return w.RowsCountValue
}

func (w *testWriter) FirstRowAt() utctime.UTCTime {
	return w.FirstRowAtValue
}

func (w *testWriter) LastRowAt() utctime.UTCTime {
	return w.LastRowAtValue
}

func (w *testWriter) CompressedSize() datasize.ByteSize {
	return w.CompressedSizeValue
}

func (w *testWriter) UncompressedSize() datasize.ByteSize {
	return w.UncompressedSizeValue
}

func (w *testWriter) WriteRow(_ time.Time, _ []any) error {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) Close(context.Context) error {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) Events() *writer.Events {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) DirPath() string {
	panic(errors.New("method should not be called"))
}

func (w *testWriter) FilePath() string {
	panic(errors.New("method should not be called"))
}
