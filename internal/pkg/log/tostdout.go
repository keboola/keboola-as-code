// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// stdoutCore writes to STDOUT output.
func stdoutCore(stdout io.Writer, logFormat LogFormat, verbose bool) zapcore.Core {
	consoleLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		// Log debug, info -> if verbose output enabled
		if verbose {
			return l == zapcore.DebugLevel || l == zapcore.InfoLevel
		}

		// Log info only
		return l == zapcore.InfoLevel
	})

	encoder := newEncoder(logFormat, verbose)

	return zapcore.NewCore(encoder, zapcore.AddSync(stdout), consoleLevels)
}
