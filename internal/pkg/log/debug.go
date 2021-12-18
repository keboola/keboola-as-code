package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

type Logger = zap.SugaredLogger

type DebugLogger struct {
	*Logger
	*ioutil.Writer
}

// NewDebugLogger returns logs as string by String() method.
// See all methods of the ioutil.Writer.
func NewDebugLogger() *DebugLogger {
	writer := ioutil.NewBufferedWriter()
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
	return &DebugLogger{
		Logger: loggerRaw.Sugar(),
		Writer: writer,
	}
}
