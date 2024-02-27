package logging

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type loggerWriter struct {
	logger log.Logger
	level  string
	buffer []byte
}

const newLine = byte(10)

func (w *loggerWriter) Write(p []byte) (n int, err error) {
	w.buffer = append(w.buffer, p...)

	if len(p) > 0 && p[len(p)-1] == newLine {
		w.logger.Log(context.Background(), w.level, string(w.buffer))
		w.buffer = []byte{}
		return 1, nil
	}

	return len(p), nil
}

func NewLoggerWriter(logger log.Logger, level string) io.Writer {
	return &loggerWriter{
		logger: logger,
		level:  level,
	}
}
