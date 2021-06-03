package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

func NewLogger(logFile *os.File, verbose bool) *zap.SugaredLogger {
	var cores []zapcore.Core
	encoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	// Log all to file
	if logFile != nil {
		fileLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool { return true })
		cores = append(cores, zapcore.NewCore(encoder, logFile, fileLevels))
	}

	// Log to console
	consoleLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		// Log all messages to console, if verbose output enabled
		if verbose {
			return true
		}

		// Otherwise log info+ messages
		return l >= zapcore.InfoLevel
	})
	cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stderr), consoleLevels))

	// Create logger
	return zap.New(zapcore.NewTee(cores...)).Sugar()
}
