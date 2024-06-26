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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
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
	writer.FormatWriter
	writeDone chan struct{}
}

func DummyWriterFactory(cfg writer.Config, out io.Writer, slice *model.Slice) (writer.FormatWriter, error) {
	return NewDummyWriter(cfg, out, slice), nil
}

func NewDummyWriter(_ writer.Config, out io.Writer, _ *model.Slice) *DummyWriter {
	return &DummyWriter{out: out}
}

func NewNotifyWriter(w writer.FormatWriter, writeDone chan struct{}) *NotifyWriter {
	return &NotifyWriter{FormatWriter: w, writeDone: writeDone}
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
	err := w.FormatWriter.WriteRecord(values)
	if err == nil {
		w.writeDone <- struct{}{}
	}
	return err
}

type WriterHelper struct {
	writeDone chan struct{}
	syncers   []*disksync.Syncer
}

func NewWriterHelper() *WriterHelper {
	return &WriterHelper{writeDone: make(chan struct{}, 100)}
}

// NewDummyWriter implements writer.FormatWriterFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *WriterHelper) NewDummyWriter(cfg writer.Config, out io.Writer, slice *model.Slice) (writer.FormatWriter, error) {
	return NewNotifyWriter(NewDummyWriter(cfg, out, slice), h.writeDone), nil
}

// NewSyncer implements disksync.SyncerFactory.
// See also ExpectWritesCount and TriggerSync methods.
func (h *WriterHelper) NewSyncer(ctx context.Context, logger log.Logger, clock clock.Clock, config disksync.Config, chain disksync.Chain, statistics disksync.StatisticsProvider,
) *disksync.Syncer {
	s := disksync.NewSyncer(ctx, logger, clock, config, chain, statistics)
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
