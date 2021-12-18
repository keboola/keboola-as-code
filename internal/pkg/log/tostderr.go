// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap/zapcore"
)

func stderrCore(stderr io.Writer, verbose bool) zapcore.Core {
	// Prefix messages with level only when verbose enabled
	levelKey := ""
	if verbose {
		levelKey = "level"
	}

	// Create encoder
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:       "msg",
		LevelKey:         levelKey,
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: "\t",
	})

	return zapcore.NewCore(encoder, zapcore.AddSync(stderr), zapcore.WarnLevel)
}
