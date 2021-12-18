package log

import (
	"go.uber.org/zap"
)

// zapLogger is default implementation of the Logger interface.
// It is wrapped zap.SugaredLogger.
type zapLogger struct {
	*zap.SugaredLogger
}

func loggerFromZap(l *zap.Logger) *zapLogger {
	return &zapLogger{SugaredLogger: l.Sugar()}
}

func (l *zapLogger) DebugWriter() *LevelWriter {
	return &LevelWriter{logger: l, level: DebugLevel}
}

func (l *zapLogger) InfoWriter() *LevelWriter {
	return &LevelWriter{logger: l, level: InfoLevel}
}

func (l *zapLogger) WarnWriter() *LevelWriter {
	return &LevelWriter{logger: l, level: WarnLevel}
}

func (l *zapLogger) ErrorWriter() *LevelWriter {
	return &LevelWriter{logger: l, level: ErrorLevel}
}
