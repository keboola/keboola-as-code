// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap/zapcore"
)

// NewServiceLogger new production zapLogger for an API or worker node.
func NewServiceLogger(writer io.Writer, verbose bool) Logger {
	var cores []zapcore.Core

	// Log to the standard logger
	cores = append(cores, writerCore(writer, verbose))

	// Create zapLogger
	return loggerFromZapCore(zapcore.NewTee(cores...))
}
