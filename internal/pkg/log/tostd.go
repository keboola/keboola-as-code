// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	stdLog "log"

	"go.uber.org/zap/zapcore"
)

type stdWrapper struct {
	logger *stdLog.Logger
}

func (w *stdWrapper) Write(p []byte) (n int, err error) {
	return len(p), w.logger.Output(2, string(p))
}

// stdCore writes to the standard logger.
func stdCore(base *stdLog.Logger, prefix string, verbose bool) zapcore.Core {
	minLevel := zapcore.InfoLevel
	if verbose {
		minLevel = zapcore.DebugLevel
	}

	// Create encoder
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:       "msg",
		LevelKey:         "level",
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: " ",
	})

	prefix = base.Prefix() + prefix
	logger := stdLog.New(base.Writer(), prefix, base.Flags())
	return zapcore.NewCore(encoder, zapcore.AddSync(&stdWrapper{logger: logger}), minLevel)
}
