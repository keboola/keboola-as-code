// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// zapLogger is default implementation of the Logger interface.
// It is wrapped zap.SugaredLogger.
type zapLogger struct {
	sugaredLogger *zap.SugaredLogger
	core          zapcore.Core
	prefix        string
}

func loggerFromZapCore(core zapcore.Core, with ...interface{}) *zapLogger {
	return &zapLogger{sugaredLogger: zap.New(core).Sugar().With(with...), core: core}
}

// With creates a child logger and adds structured context to it.
func (l *zapLogger) With(args ...interface{}) Logger {
	return loggerFromZapCore(l.core, args...)
}

// AddPrefix creates a child logger with added prefix.
func (l *zapLogger) AddPrefix(prefix string) Logger {
	prefix = l.prefix + prefix
	clone := l.With(PrefixKey, prefix).(*zapLogger)
	clone.prefix = prefix
	return clone
}

func (l *zapLogger) LogCtx(ctx context.Context, level string, args ...any) {
	switch level {
	case "debug", "DEBUG":
		l.sugaredLogger.Debug(args...)
	case "info", "INFO":
		l.sugaredLogger.Info(args...)
	case "warn", "WARN":
		l.sugaredLogger.Warn(args...)
	case "error", "ERROR":
		l.sugaredLogger.Error(args...)
	case "dpanic", "DPANIC":
		l.sugaredLogger.DPanic(args...)
	case "panic", "PANIC":
		l.sugaredLogger.Panic(args...)
	case "fatal", "FATAL":
		l.sugaredLogger.Fatal(args...)
	default:
		l.sugaredLogger.Info(args...)
	}
}

func (l *zapLogger) DebugCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Debug(args...)
}

func (l *zapLogger) InfoCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Info(args...)
}

func (l *zapLogger) WarnCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Warn(args...)
}

func (l *zapLogger) ErrorCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Error(args...)
}

func (l *zapLogger) DebugfCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Debugf(template, args...)
}

func (l *zapLogger) InfofCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Infof(template, args...)
}

func (l *zapLogger) WarnfCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Warnf(template, args...)
}

func (l *zapLogger) ErrorfCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Errorf(template, args...)
}

func (l *zapLogger) Sync() error {
	return l.sugaredLogger.Sync()
}

func (l *zapLogger) DebugWriter() *LevelWriter {
	return &LevelWriter{logger: l.sugaredLogger, level: DebugLevel}
}

func (l *zapLogger) InfoWriter() *LevelWriter {
	return &LevelWriter{logger: l.sugaredLogger, level: InfoLevel}
}

func (l *zapLogger) WarnWriter() *LevelWriter {
	return &LevelWriter{logger: l.sugaredLogger, level: WarnLevel}
}

func (l *zapLogger) ErrorWriter() *LevelWriter {
	return &LevelWriter{logger: l.sugaredLogger, level: ErrorLevel}
}

func (l *zapLogger) zapCore() zapcore.Core {
	return l.core
}
