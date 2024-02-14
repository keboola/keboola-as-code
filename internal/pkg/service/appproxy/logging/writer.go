package logging

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type loggerWriter struct {
	logger log.Logger
	level  string
}

func (w loggerWriter) Write(p []byte) (n int, err error) {
	w.logger.LogCtx(context.Background(), w.level, string(p))
	return len(p), nil
}

func NewLoggerWriter(logger log.Logger, level string) io.Writer {
	return &loggerWriter{
		logger: logger,
		level:  level,
	}
}
