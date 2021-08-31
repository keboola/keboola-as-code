package log

import (
	"fmt"
	"io"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type WriteCloser struct {
	level  zapcore.Level
	logger *zap.SugaredLogger
}

// Write messages with the defined level to logger.
func (w *WriteCloser) Write(p []byte) (n int, err error) {
	lines := strings.TrimRight(string(p), "\n")
	for _, line := range strings.Split(lines, "\n") {
		msg := strings.TrimRight(line, "\n")
		switch w.level {
		case zapcore.DebugLevel:
			w.logger.Debug(msg)
		case zapcore.InfoLevel:
			w.logger.Info(msg)
		case zapcore.WarnLevel:
			w.logger.Warn(msg)
		default:
			w.logger.Error(msg)
		}
	}
	return len(p), nil
}

func (w *WriteCloser) Close() error {
	return w.logger.Sync()
}

func (w *WriteCloser) WriteString(s string) (n int, err error) {
	return w.Write([]byte(s))
}
func (w *WriteCloser) WriteNoErr(p []byte) {
	if _, err := w.Write(p); err != nil {
		panic(fmt.Errorf("cannot write: %s", err))
	}
}

func (w *WriteCloser) WriteStringNoErr(s string) {
	if _, err := w.WriteString(s); err != nil {
		panic(fmt.Errorf("cannot write: %s", err))
	}
}

func ToDebugWriter(l *zap.SugaredLogger) *WriteCloser {
	return &WriteCloser{zapcore.DebugLevel, l}
}

func ToInfoWriter(l *zap.SugaredLogger) *WriteCloser {
	return &WriteCloser{zapcore.InfoLevel, l}
}

func ToWarnWriter(l *zap.SugaredLogger) *WriteCloser {
	return &WriteCloser{zapcore.WarnLevel, l}
}

func ToErrorWriter(l *zap.SugaredLogger) *WriteCloser {
	return &WriteCloser{zapcore.ErrorLevel, l}
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
	cores = append(cores, getStderrCore(stderr, verbose))

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

	// Prefix messages with level only when verbose enabled
	levelKey := ""
	if verbose {
		levelKey = "level"
	}

	// Create encoder
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:       "msg",
		LevelKey:         levelKey,
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: "\t",
	})

	return zapcore.NewCore(encoder, zapcore.AddSync(stdout), consoleLevels)
}

func getStderrCore(stderr io.Writer, verbose bool) zapcore.Core {
	// Prefix messages with level only when verbose enabled
	levelKey := ""
	if verbose {
		levelKey = "level"
	}

	// Create encoder
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:       "msg",
		LevelKey:         levelKey,
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: "\t",
	})

	return zapcore.NewCore(encoder, zapcore.AddSync(stderr), zapcore.WarnLevel)
}
