// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap/zapcore"
)

// NewCliLogger new production zapLogger for CLI.
func NewCliLogger(stdout io.Writer, stderr io.Writer, logFile *File, logFormat LogFormat, verbose bool) Logger {
	var cores []zapcore.Core

	// Log to file
	if logFile != nil {
		cores = append(cores, fileCore(logFile))
	}

	// Log to stdout
	cores = append(cores, stdoutCore(stdout, logFormat, verbose))

	// Log to stderr
	cores = append(cores, stderrCore(stderr, logFormat, verbose))

	// Create zapLogger
	return newLoggerFromZapCore(zapcore.NewTee(cores...))
}
