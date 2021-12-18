package log

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewCliLogger new production zapLogger.
func NewCliLogger(stdout io.Writer, stderr io.Writer, logFile *File, verbose bool) Logger {
	var cores []zapcore.Core

	// Log to file
	if logFile != nil {
		cores = append(cores, fileCore(logFile))
	}

	// Log to stdout
	cores = append(cores, stdoutCore(stdout, verbose))

	// Log to stderr
	cores = append(cores, stderrCore(stderr, verbose))

	// Create zapLogger
	return loggerFromZap(zap.New(zapcore.NewTee(cores...)))
}
