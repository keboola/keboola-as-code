// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap/zapcore"
)

// writerCore writes to a writer.
func writerCore(stderr io.Writer, verbose bool) zapcore.Core {
	minLevel := zapcore.InfoLevel
	if verbose {
		minLevel = zapcore.DebugLevel
	}

	return zapcore.NewCore(newJSONEncoder(), zapcore.AddSync(stderr), minLevel)
}
