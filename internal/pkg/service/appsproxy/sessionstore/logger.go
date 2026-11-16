package sessionstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// go-redis logs through Printf only, without a level, so the level is taken from the text.
type redisLogger struct {
	logger log.Logger
}

func newRedisLogger(logger log.Logger) *redisLogger {
	return &redisLogger{logger: logger}
}

func (l *redisLogger) Printf(ctx context.Context, format string, v ...any) {
	message := fmt.Sprintf(format, v...)
	if strings.Contains(message, "failed") {
		l.logger.Warn(ctx, message)
		return
	}
	l.logger.Info(ctx, message)
}
