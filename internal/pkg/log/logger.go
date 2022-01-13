// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// zapLogger is default implementation of the Logger interface.
// It is wrapped zap.SugaredLogger.
type zapLogger struct {
	*zap.SugaredLogger
	core zapcore.Core
}

func loggerFromZapCore(core zapcore.Core, with ...interface{}) *zapLogger {
	return &zapLogger{SugaredLogger: zap.New(core).Sugar().With(with...), core: core}
}

// With creates a child logger and adds structured context to it.
func (l *zapLogger) With(args ...interface{}) Logger {
	return loggerFromZapCore(l.core, args...)
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

func (l *zapLogger) zapCore() zapcore.Core {
	return l.core
}
