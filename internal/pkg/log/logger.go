// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/ctxattr"
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

func formatMessage(args ...any) string {
	if len(args) == 1 {
		if str, ok := args[0].(string); ok {
			return str
		}
	}
	return fmt.Sprint(args...)
}

func (l *zapLogger) LogCtx(ctx context.Context, level string, args ...any) {
	message := formatMessage(args...)
	fields := ctxattr.ZapFields(ctx)
	switch level {
	case "debug", "DEBUG":
		l.sugaredLogger.Desugar().Debug(message, fields...)
	case "info", "INFO":
		l.sugaredLogger.Desugar().Info(message, fields...)
	case "warn", "WARN":
		l.sugaredLogger.Desugar().Warn(message, fields...)
	case "error", "ERROR":
		l.sugaredLogger.Desugar().Error(message, fields...)
	case "dpanic", "DPANIC":
		l.sugaredLogger.Desugar().DPanic(message, fields...)
	case "panic", "PANIC":
		l.sugaredLogger.Desugar().Panic(message, fields...)
	case "fatal", "FATAL":
		l.sugaredLogger.Desugar().Fatal(message, fields...)
	default:
		l.sugaredLogger.Desugar().Info(message, fields...)
	}
}

func (l *zapLogger) DebugCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Desugar().Debug(formatMessage(args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) InfoCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Desugar().Info(formatMessage(args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) WarnCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Desugar().Warn(formatMessage(args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) ErrorCtx(ctx context.Context, args ...any) {
	l.sugaredLogger.Desugar().Error(formatMessage(args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) DebugfCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Debug(fmt.Sprintf(template, args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) InfofCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Info(fmt.Sprintf(template, args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) WarnfCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Warn(fmt.Sprintf(template, args...), ctxattr.ZapFields(ctx)...)
}

func (l *zapLogger) ErrorfCtx(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Error(fmt.Sprintf(template, args...), ctxattr.ZapFields(ctx)...)
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
