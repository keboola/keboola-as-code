package ioutil

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

// Writer is a simple buffer writer for testing.
// It implements these interfaces:
// - io.Writer
// - io.WriteCloser
// - io.Closer
// - terminal.FileWriter.
type Writer struct {
	mutex   *sync.Mutex
	writers []io.Writer
	buffer  *bytes.Buffer
}

func NewBufferedWriter() *Writer {
	var buffer bytes.Buffer
	return &Writer{&sync.Mutex{}, []io.Writer{bufio.NewWriter(&buffer)}, &buffer}
}

// ConnectTo allows writes to multiple targets.
func (w *Writer) ConnectTo(writer io.Writer) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.writers = append(w.writers, writer)
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for _, writer := range w.writers {
		if _, err = writer.Write(p); err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

func (w *Writer) WriteString(s string) (n int, err error) {
	return w.Write([]byte(s))
}

func (w *Writer) Flush() (err error) {
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

func (w *Writer) Close() error {
	return w.Flush()
}

// Fd fake terminal file descriptor.
func (*Writer) Fd() uintptr {
	return os.Stdout.Fd()
}

func (w *Writer) Truncate() {
	if err := w.Flush(); err != nil {
		panic(fmt.Errorf("cannot flush utils log writer"))
	}
	w.buffer.Truncate(0)
}

func (w *Writer) String() string {
	if err := w.Flush(); err != nil {
		panic(fmt.Errorf("cannot flush utils log writer"))
	}
	str := w.buffer.String()
	w.Truncate()
	return str
}
