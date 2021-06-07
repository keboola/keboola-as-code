package utils

import (
	"bufio"
	"bytes"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewBufferWriter() (*bufio.Writer, *bytes.Buffer) {
	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	return writer, &buffer
}

func NewBufferReader() (*bufio.Reader, *bytes.Buffer) {
	var buffer bytes.Buffer
	reader := bufio.NewReader(&buffer)
	return reader, &buffer
}

func NewDebugLogger() (*zap.SugaredLogger, *bufio.Writer, *bytes.Buffer) {
	writer, buffer := NewBufferWriter()
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

	return logger, writer, buffer
}
