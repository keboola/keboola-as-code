// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// fileCore writes to a logFile.
func fileCore(logFile *File) zapcore.Core {
	// Log all
	fileLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool { return true })

	// Log time, level, msg
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		MessageKey:       "msg",
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: "\t",
	})
	return zapcore.NewCore(encoder, logFile.File(), fileLevels)
}
