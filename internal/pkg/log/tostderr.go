// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap/zapcore"
)

// stderrCore writes to STDERR output.
func stderrCore(stderr io.Writer, logFormat LogFormat, verbose bool) zapcore.Core {
	encoder := newEncoder(logFormat, verbose)

	return zapcore.NewCore(encoder, zapcore.AddSync(stderr), zapcore.WarnLevel)
}
