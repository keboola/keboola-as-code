package log

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type LevelWriter struct {
	logger baseLogger
	level  zapcore.Level
}

// Write messages with the defined level to zapLogger.
func (w *LevelWriter) Write(p []byte) (n int, err error) {
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

func (w *LevelWriter) WriteNoErr(p []byte) {
	if _, err := w.Write(p); err != nil {
		panic(errors.Errorf("cannot write: %w", err))
	}
}

func (w *LevelWriter) WriteString(s string) {
	w.WriteNoErr([]byte(s))
}

func (w *LevelWriter) WriteStringIndent(indent int, s string) {
	w.WriteString(strings.Repeat("  ", indent) + s)
}

func (w *LevelWriter) Writef(format string, a ...any) {
	w.WriteNoErr([]byte(fmt.Sprintf(format, a...)))
}

func (w *LevelWriter) Close() error {
	return w.logger.Sync()
}
