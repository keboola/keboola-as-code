package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

func NewLogger(logFile *os.File, verbose bool) *zap.SugaredLogger {
	var cores []zapcore.Core

	// Log to file
	if logFile != nil {
		cores = append(cores, getFileCore(logFile))
	}

	// Log to console
	cores = append(cores, getConsoleCore(verbose))

	// Create logger
	return zap.New(zapcore.NewTee(cores...)).Sugar()
}

func getFileCore(logFile *os.File) zapcore.Core {
	// Log all
	fileLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool { return true })

	// Log time, level, msg
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		TimeKey:     "ts",
		LevelKey:    "level",
		MessageKey:  "msg",
		EncodeLevel: zapcore.CapitalLevelEncoder,
	})
	return zapcore.NewCore(encoder, logFile, fileLevels)
}

func getConsoleCore(verbose bool) zapcore.Core {
	consoleLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		// Log all messages, if verbose output enabled
		if verbose {
			return true
		}

		// Otherwise log info+ messages
		return l >= zapcore.InfoLevel
	})

	// Info is logged without prefix, other levels with prefix
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		LevelKey:    "level",
		MessageKey:  "msg",
		EncodeLevel: func (l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			if l != zapcore.InfoLevel {
				enc.AppendString(l.CapitalString())
			}
		},
	})

	return zapcore.NewCore(encoder, zapcore.AddSync(os.Stderr), consoleLevels)
}
