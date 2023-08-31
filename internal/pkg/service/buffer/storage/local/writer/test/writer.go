package test

import (
	"bytes"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/base"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"testing"
	"time"
)

// SliceWriter implements a simple writer implementing writer.SliceWriter for tests.
// The writer writes row values to one line as strings, separated by comma.
type SliceWriter struct {
	base *base.Writer
	// CompressedSizeValue defines value of the CompressedSize getter.
	CompressedSizeValue datasize.ByteSize
	// UncompressedSizeValue defines value of the UncompressedSize getter.
	UncompressedSizeValue datasize.ByteSize
	// RowsCounter counts successfully written rows.
	RowsCounter *atomic.Uint64
	// CloseError simulates error in the Close method.
	CloseError error
	// WriteDone signals completion of the write operation and the start of waiting for disk synchronization.
	// See the WriteRow method.
	WriteDone chan struct{}
}

func NewSliceWriter(b *base.Writer) *SliceWriter {
	return &SliceWriter{
		base:        b,
		WriteDone:   make(chan struct{}, 100),
		RowsCounter: atomic.NewUint64(0),
	}
}

func (w *SliceWriter) WriteRow(values []any) error {
	var s bytes.Buffer
	for i, v := range values {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cast.ToString(v))
	}
	s.WriteString("\n")

	_, notifier, err := w.base.WriteWithNotify(s.Bytes())
	w.base.AddWriteOp(1)

	w.WriteDone <- struct{}{}

	// Wait for sync
	if err == nil {
		err = notifier.Wait()
	}

	return err
}

func (w *SliceWriter) RowsCount() uint64 {
	return w.RowsCounter.Load()
}

func (w *SliceWriter) CompressedSize() datasize.ByteSize {
	return w.CompressedSizeValue
}

func (w *SliceWriter) UncompressedSize() datasize.ByteSize {
	return w.UncompressedSizeValue
}

func (w *SliceWriter) SliceKey() storage.SliceKey {
	return w.base.SliceKey()
}

func (w *SliceWriter) DirPath() string {
	return w.base.DirPath()
}

func (w *SliceWriter) FilePath() string {
	return w.base.FilePath()
}

func (w *SliceWriter) Close() error {
	if err := w.base.Close(); err != nil {
		return err
	}
	return w.CloseError
}

func (w *SliceWriter) ExpectWritesCount(t testing.TB, n int) {
	t.Logf(`waiting for %d writes`, n)
	for i := 0; i < n; i++ {
		select {
		case <-w.WriteDone:
			t.Logf(`write %d done`, i+1)
		case <-time.After(2 * time.Second):
			assert.FailNow(t, "timeout")
			return
		}
	}
	t.Logf(`all writes done`)
}

func (w *SliceWriter) TriggerSync(t testing.TB) {
	t.Logf("trigger sync")
	assert.NoError(t, w.base.TriggerSync(true).Wait())
	t.Logf("sync done")
}
