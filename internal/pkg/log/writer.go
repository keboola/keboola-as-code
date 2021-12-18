package log

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
)

type LevelWriter struct {
	logger baseLogger
	level  zapcore.Level
}

// Write messages with the defined level to zapLogger.
func (w *LevelWriter) write(p []byte) (n int, err error) {
	lines := strings.TrimRight(string(p), "\n")
	for _, line := range strings.Split(lines, "\n") {
		msg := strings.TrimRight(line, "\n")
		switch w.level {
		case DebugLevel:
			w.logger.Debug(msg)
		case InfoLevel:
			w.logger.Info(msg)
		case WarnLevel:
			w.logger.Warn(msg)
		case ErrorLevel:
			w.logger.Error(msg)
		default:
			w.logger.Info(msg)
		}
	}
	return len(p), nil
}

func (w *LevelWriter) Write(p []byte) {
	if _, err := w.write(p); err != nil {
		panic(fmt.Errorf("cannot write: %w", err))
	}
}

func (w *LevelWriter) WriteString(s string) {
	w.Write([]byte(s))
}

func (w *LevelWriter) WriteStringIndent(s string, indent int) {
	w.WriteString(strings.Repeat("  ", indent) + s)
}

func (w *LevelWriter) Close() error {
	return w.logger.Sync()
}
