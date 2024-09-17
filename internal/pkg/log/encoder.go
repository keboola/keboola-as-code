package log

import (
	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func newConsoleEncoder(verbose bool) zapcore.Encoder {
	// Prefix messages with level only when verbose enabled
	levelKey := ""
	if verbose {
		levelKey = "level"
	}

	// Create encoder
	return newNoFieldsEncoder(
		zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
			MessageKey:       "msg",
			LevelKey:         levelKey,
			EncodeLevel:      zapcore.CapitalLevelEncoder,
			ConsoleSeparator: "\t",
		}),
	)
}

func newJSONEncoder() zapcore.Encoder {
	// Log time, level, msg
	return zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		MessageKey:    "message",
		CallerKey:     "caller",
		StacktraceKey: "stack",
		EncodeLevel:   zapcore.LowercaseLevelEncoder,
		EncodeTime:    zapcore.ISO8601TimeEncoder,
	})
}

func newEncoder(logFormat LogFormat, verbose bool) zapcore.Encoder {
	switch logFormat {
	case LogFormatConsole:
		return newConsoleEncoder(verbose)
	case LogFormatJSON:
		return newJSONEncoder()
	default:
		panic(errors.Errorf(`unexpected log.LogFormat = %v`, logFormat))
	}
}
