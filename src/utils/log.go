package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

type Writer struct {
	writer *bufio.Writer
	buffer *bytes.Buffer
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *Writer) WriteString(s string) (n int, err error) {
	return w.writer.WriteString(s)
}

func (w *Writer) Flush() error {
	return w.writer.Flush()
}

func (*Writer) Close() error { return nil }

// Fd fake terminal file descriptor
func (*Writer) Fd() uintptr {
	return os.Stdout.Fd()
}

func (w *Writer) String() string {
	err := w.writer.Flush()
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

// Fd fake terminal file descriptor
func (*Reader) Fd() uintptr {
	return os.Stdin.Fd()
}

func NewBufferWriter() *Writer {
	var buffer bytes.Buffer
	return &Writer{bufio.NewWriter(&buffer), &buffer}
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
