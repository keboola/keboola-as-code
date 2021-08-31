package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Writer struct {
	mutex   *sync.Mutex
	writers []io.Writer
	buffer  *bytes.Buffer
}

// ConnectTo allows write to multiple targets.
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
	for _, writer := range w.writers {
		if buffWriter, ok := writer.(*bufio.Writer); ok {
			if err = buffWriter.Flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (*Writer) Close() error { return nil }

// Fd fake terminal file descriptor.
func (*Writer) Fd() uintptr {
	return os.Stdout.Fd()
}

func (w *Writer) String() string {
	err := w.Flush()
	if err != nil {
		panic(fmt.Errorf("cannot flush utils log writer"))
	}
	return w.buffer.String()
}

type Reader struct {
	Reader *bufio.Reader
	Buffer *bytes.Buffer
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.Reader.Read(p)
}

func (*Reader) Close() error { return nil }

// Fd fake terminal file descriptor.
func (*Reader) Fd() uintptr {
	return os.Stdin.Fd()
}

func NewBufferWriter() *Writer {
	var buffer bytes.Buffer
	return &Writer{&sync.Mutex{}, []io.Writer{bufio.NewWriter(&buffer)}, &buffer}
}

func NewBufferReader() *Reader {
	var buffer bytes.Buffer
	return &Reader{bufio.NewReader(&buffer), &buffer}
}

func NewDebugLogger() (*zap.SugaredLogger, *Writer) {
	writer := NewBufferWriter()
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		MessageKey:       "msg",
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: "  ",
	}
	loggerRaw := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(writer),
		zapcore.DebugLevel,
	))
	logger := loggerRaw.Sugar()

	return logger, writer
}
