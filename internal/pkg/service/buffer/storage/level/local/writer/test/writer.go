package test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/count"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Writer implements a simple writer implementing writer.Writer for tests.
// The writer writes row values to one line as strings, separated by comma.
type Writer struct {
	base    *writer.BaseWriter
	writeWg *sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	// CompressedSizeValue defines value of the CompressedSize getter.
	CompressedSizeValue datasize.ByteSize
	// UncompressedSizeValue defines value of the UncompressedSize getter.
	UncompressedSizeValue datasize.ByteSize
	// RowsCounter counts successfully written rows.
	RowsCounter *count.Counter
	// CloseError simulates error in the Close method.
	CloseError error
	// WriteDone signals completion of the write operation and the start of waiting for disk synchronization.
	// See the WriteRow method.
	WriteDone chan struct{}
}

func NewSliceWriter(b *writer.BaseWriter) *Writer {
	w := &Writer{
		base:        b,
		writeWg:     &sync.WaitGroup{},
		WriteDone:   make(chan struct{}, 100),
		RowsCounter: count.NewCounter(),
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())

	return w
}

func (w *Writer) WriteRow(timestamp time.Time, values []any) error {
	// Block Close method
	w.writeWg.Add(1)
	defer w.writeWg.Done()

	// Check if the writer is closed
	if err := w.ctx.Err(); err != nil {
		return errors.Errorf(`test writer is closed: %w`, err)
	}

	var s bytes.Buffer
	for i, v := range values {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cast.ToString(v))
	}
	s.WriteString("\n")

	// Write
	_, notifier, err := w.base.WriteWithNotify(s.Bytes())
	if err != nil {
		return err
	}

	// Increments number of high-level writes in progress
	w.base.AddWriteOp(1)

	// Signal the completion of write operation and waiting for sync
	w.WriteDone <- struct{}{}

	// Wait for sync and return sync error, if any
	if err = notifier.Wait(); err != nil {
		return err
	}

	// Increase the count of successful writes
	w.RowsCounter.Add(timestamp, 1)
	return nil
}

func (w *Writer) RowsCount() uint64 {
	return w.RowsCounter.Count()
}

func (w *Writer) FirstRowAt() utctime.UTCTime {
	return w.RowsCounter.FirstAt()
}

func (w *Writer) LastRowAt() utctime.UTCTime {
	return w.RowsCounter.LastAt()
}

func (w *Writer) CompressedSize() datasize.ByteSize {
	return w.CompressedSizeValue
}

func (w *Writer) UncompressedSize() datasize.ByteSize {
	return w.UncompressedSizeValue
}

func (w *Writer) SliceKey() storage.SliceKey {
	return w.base.SliceKey()
}

func (w *Writer) DirPath() string {
	return w.base.DirPath()
}

func (w *Writer) FilePath() string {
	return w.base.FilePath()
}

func (w *Writer) Events() *writer.Events {
	return w.base.Events()
}

func (w *Writer) Close() error {
	// Prevent new writes
	w.cancel()

	// Close the chain
	err := w.base.Close()

	// Wait for running writes
	w.writeWg.Wait()

	if err != nil {
		return err
	}

	return w.CloseError
}

func (w *Writer) ExpectWritesCount(tb testing.TB, n int) {
	tb.Helper()
	tb.Logf(`waiting for %d writes`, n)
	for i := 0; i < n; i++ {
		select {
		case <-w.WriteDone:
			tb.Logf(`write %d done`, i+1)
		case <-time.After(2 * time.Second):
			assert.FailNow(tb, "timeout")
			return
		}
	}
	tb.Logf(`all writes done`)
}

func (w *Writer) TriggerSync(tb testing.TB) {
	tb.Helper()
	tb.Logf("trigger sync")
	assert.NoError(tb, w.base.TriggerSync(true).Wait())
	tb.Logf("sync done")
}
