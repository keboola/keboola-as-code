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
	core   zapcore.Core
	prefix string
}

func loggerFromZapCore(core zapcore.Core, with ...interface{}) *zapLogger {
	return &zapLogger{SugaredLogger: zap.New(core).Sugar().With(with...), core: core}
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

func (l *zapLogger) Log(level string, args ...any) {
	switch level {
	case "debug", "DEBUG":
		l.Debug(args...)
	case "info", "INFO":
		l.Info(args...)
	case "warn", "WARN":
		l.Warn(args...)
	case "error", "ERROR":
		l.Error(args...)
	case "dpanic", "DPANIC":
		l.DPanic(args...)
	case "panic", "PANIC":
		l.Panic(args...)
	case "fatal", "FATAL":
		l.Fatal(args...)
	default:
		l.Info(args...)
	}
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
