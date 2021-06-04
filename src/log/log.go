package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"os"
	"strings"
)

type InfoWriter struct {
	logger *zap.SugaredLogger
}

type WarnWriter struct {
	logger *zap.SugaredLogger
}

// Write message with INFO level to logger
func (w *InfoWriter) Write(p []byte) (n int, err error) {
	lines := strings.TrimRight(string(p), "\n")
	for _, line := range strings.Split(lines, "\n") {
		w.logger.Info(strings.TrimRight(line, "\n"))
	}
	return len(p), nil
}

// Write message with WARN level to logger
func (w *WarnWriter) Write(p []byte) (n int, err error) {
	lines := strings.TrimRight(string(p), "\n")
	for _, line := range strings.Split(lines, "\n") {
		w.logger.Warn(strings.TrimRight(line, "\n"))
	}
	return len(p), nil
}

func ToInfoWriter(l *zap.SugaredLogger) *InfoWriter {
	return &InfoWriter{l}
}

func ToWarnWriter(l *zap.SugaredLogger) *WarnWriter {
	return &WarnWriter{l}
}

func NewLogger(stdout io.Writer, stderr io.Writer, logFile *os.File, verbose bool) *zap.SugaredLogger {
	var cores []zapcore.Core

	// Log to file
	if logFile != nil {
		cores = append(cores, getFileCore(logFile))
	}

	// Log to stdout
	cores = append(cores, getStdoutCore(stdout, verbose))

	// Log to stderr
	cores = append(cores, getStderrCore(stderr))

	// Create logger
	return zap.New(zapcore.NewTee(cores...)).Sugar()
}

func getFileCore(logFile *os.File) zapcore.Core {
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
	return zapcore.NewCore(encoder, logFile, fileLevels)
}

func getStdoutCore(stdout io.Writer, verbose bool) zapcore.Core {
	consoleLevels := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		// Log debug, info -> if verbose output enabled
		if verbose {
			return l == zapcore.DebugLevel || l == zapcore.InfoLevel
		}

		// Log info only
		return l == zapcore.InfoLevel
	})

	// Info is logged without prefix, other levels with prefix
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:       "msg",
		ConsoleSeparator: "  ",
	})

	return zapcore.NewCore(encoder, zapcore.AddSync(stdout), consoleLevels)
}

func getStderrCore(stderr io.Writer) zapcore.Core {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:       "msg",
		ConsoleSeparator: "  ",
	})
	return zapcore.NewCore(encoder, zapcore.AddSync(stderr), zapcore.WarnLevel)
}
