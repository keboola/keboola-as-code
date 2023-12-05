// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap/zapcore"
)

// fileCore writes to a logFile.
func fileCore(logFile *File) zapcore.Core {
	// Log all
	fileLevels := zapcore.DebugLevel

	// Log time, level, msg
	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:     "time",
		LevelKey:    "level",
		MessageKey:  "message",
		EncodeLevel: zapcore.LowercaseLevelEncoder,
		EncodeTime:  zapcore.ISO8601TimeEncoder,
	})

	return zapcore.NewCore(encoder, logFile.File(), fileLevels)
}
