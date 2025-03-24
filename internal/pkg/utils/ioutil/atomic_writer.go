package ioutil

import (
	"bufio"
	"bytes"
	"io"
	"os"

	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AtomicWriter is a simple buffer writer for testing.
// It implements these interfaces:
// - io.Writer
// - io.WriteCloser
// - io.Closer
// - terminal.FileWriter.
type AtomicWriter struct {
	mutex   *deadlock.Mutex
	writers []io.Writer
	buffer  *bytes.Buffer
}

func NewAtomicWriter() *AtomicWriter {
	var buffer bytes.Buffer
	return &AtomicWriter{&deadlock.Mutex{}, []io.Writer{bufio.NewWriter(&buffer)}, &buffer}
}

// ConnectTo allows writes to multiple targets.
func (w *AtomicWriter) ConnectTo(writer io.Writer) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.writers = append(w.writers, writer)
}

func (w *AtomicWriter) Write(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for _, writer := range w.writers {
		if _, err = writer.Write(p); err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

func (w *AtomicWriter) WriteString(s string) (n int, err error) {
	return w.Write([]byte(s))
}

func (w *AtomicWriter) Sync() (err error) {
	return w.Flush()
}

func (w *AtomicWriter) Flush() (err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for _, writer := range w.writers {
		if buffWriter, ok := writer.(*bufio.Writer); ok {
			if err = buffWriter.Flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *AtomicWriter) Close() error {
	return w.Flush()
}

// Fd fake terminal file descriptor.
func (*AtomicWriter) Fd() uintptr {
	return os.Stdout.Fd() // nolint:forbidigo
}

func (w *AtomicWriter) Truncate() {
	if err := w.Flush(); err != nil {
		panic(errors.New("cannot flush utils log writer"))
	}
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.buffer.Truncate(0)
}

func (w *AtomicWriter) String() string {
	if err := w.Flush(); err != nil {
		panic(errors.New("cannot flush utils log writer"))
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.buffer.String()
}

func (w *AtomicWriter) StringAndTruncate() string {
	str := w.String()
	w.Truncate()
	return str
}
