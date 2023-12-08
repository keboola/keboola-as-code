// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap/zapcore"
)

// fileCore writes to a logFile.
func fileCore(logFile *File) zapcore.Core {
	// Log all
	fileLevels := zapcore.DebugLevel

	// Log file intentionally always uses json output.
	encoder := newJSONEncoder()

	return zapcore.NewCore(encoder, logFile.File(), fileLevels)
}
