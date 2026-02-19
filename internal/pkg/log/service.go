// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap/zapcore"
)

// NewServiceLogger new production zapLogger for an API or worker node.
func NewServiceLogger(writer io.Writer, verbose bool) Logger {
	cores := make([]zapcore.Core, 0, 1)

	// Log to the standard logger
	cores = append(cores, writerCore(writer, verbose))

	// Create zapLogger
	return newLoggerFromZapCore(zapcore.NewTee(cores...))
}
