package test

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/base"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

type baseWriter = base.Writer

// SliceWriter implements a simple writer implementing writer.SliceWriter for tests.
// The writer writes row values to one line as strings, separated by comma.
type SliceWriter struct {
	baseWriter
	// CompressedSizeValue defines value of the CompressedSize getter.
	CompressedSizeValue datasize.ByteSize
	// UncompressedSizeValue defines value of the UncompressedSize getter.
	UncompressedSizeValue datasize.ByteSize
	// CloseError simulates error in the Close method.
	CloseError error
	// WriteDone signals completion of the write operation and the start of waiting for disk synchronization.
	// See the WriteRow method.
	WriteDone chan struct{}
}

func NewSliceWriter(b base.Writer) *SliceWriter {
	return &SliceWriter{baseWriter: b, WriteDone: make(chan struct{}, 100)}
}

func (w *SliceWriter) WriteRow(values []any) error {
	var s strings.Builder
	for i, v := range values {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cast.ToString(v))
	}
	s.WriteString("\n")

	var err error
	notifier := w.Syncer().DoWithNotifier(func() {
		_, err = w.Chain().WriteString(s.String())
	})

	w.WriteDone <- struct{}{}

	if err != nil {
		return err
	}

	return notifier.Wait()
}

func (w *SliceWriter) CompressedSize() datasize.ByteSize {
	return w.CompressedSizeValue
}

func (w *SliceWriter) UncompressedSize() datasize.ByteSize {
	return w.UncompressedSizeValue
}

func (w *SliceWriter) Close() error {
	if err := w.baseWriter.Close(); err != nil {
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
	w.baseWriter.Syncer().SyncAndWait()
	t.Logf("sync done")
}
