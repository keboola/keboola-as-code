package test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/format"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// DummyWriter implements a simple writer implementing writer.Writer for tests.
// The writer writes row values to one line as strings, separated by comma.
// Row is separated by the new line.
type DummyWriter struct {
	out        io.Writer
	FlushError error
	CloseError error
}

// NotifyWriter notifies about successful writes.
type NotifyWriter struct {
	format.Writer
	writeDone chan struct{}
}

func DummyWriterFactory(cfg format.Config, out io.Writer, slice *model.Slice) (format.Writer, error) {
	return NewDummyWriter(cfg, out, slice), nil
}

func NewDummyWriter(_ format.Config, out io.Writer, _ *model.Slice) *DummyWriter {
	return &DummyWriter{out: out}
}

func NewNotifyWriter(w format.Writer, writeDone chan struct{}) *NotifyWriter {
	return &NotifyWriter{Writer: w, writeDone: writeDone}
}

func (w *DummyWriter) WriteRecord(values []any) error {
	var s bytes.Buffer
	for i, v := range values {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cast.ToString(v))
	}
	s.WriteString("\n")

	_, err := w.out.Write(s.Bytes())
	return err
}

func (w *DummyWriter) Flush() error {
	return w.FlushError
}

func (w *DummyWriter) Close() error {
	return w.CloseError
}

func (w *NotifyWriter) WriteRecord(values []any) error {
	err := w.Writer.WriteRecord(values)
	if err == nil {
		w.writeDone <- struct{}{}
	}
	return err
}

type WriterHelper struct {
	writeDone chan struct{}
	syncers   []*writesync.Syncer
}

func NewWriterHelper() *WriterHelper {
	return &WriterHelper{writeDone: make(chan struct{}, 100)}
}

// NewDummyWriter implements writer.WriterFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *WriterHelper) NewDummyWriter(cfg format.Config, out io.Writer, slice *model.Slice) (format.Writer, error) {
	return NewNotifyWriter(NewDummyWriter(cfg, out, slice), h.writeDone), nil
}

// NewSyncer implements writesync.SyncerFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *WriterHelper) NewSyncer(ctx context.Context, logger log.Logger, clock clock.Clock, config writesync.Config, chain writesync.Chain, statistics writesync.StatisticsProvider,
) *writesync.Syncer {
	s := writesync.NewSyncer(ctx, logger, clock, config, chain, statistics)
	h.syncers = append(h.syncers, s)
	return s
}

func (h *WriterHelper) ExpectWritesCount(tb testing.TB, n int) {
	tb.Helper()
	tb.Logf(`waiting for %d writes`, n)
	for i := 0; i < n; i++ {
		select {
		case <-h.writeDone:
			tb.Logf(`write %d done`, i+1)
		case <-time.After(2 * time.Second):
			assert.FailNow(tb, "timeout")
			return
		}
	}
	tb.Logf(`all writes done`)
}

func (h *WriterHelper) TriggerSync(tb testing.TB) {
	tb.Helper()
	tb.Logf("trigger sync")

	wg := &sync.WaitGroup{}
	for _, s := range h.syncers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(tb, s.TriggerSync(context.Background(), true).Wait())
		}()
	}
	wg.Wait()

	tb.Logf("sync done")
}
