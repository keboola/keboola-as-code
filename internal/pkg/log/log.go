package log

import (
	"context"
	"io"

	"go.uber.org/zap/zapcore"
)

const (
	DebugLevel = zapcore.DebugLevel
	InfoLevel  = zapcore.InfoLevel
	WarnLevel  = zapcore.WarnLevel
	ErrorLevel = zapcore.ErrorLevel
)

type Logger interface {
	contextLogger
	toWriter
	withFields
	withPrefix
}

type loggerWithZapCore interface {
	Logger
	zapCore() zapcore.Core
}

// DebugLogger returns logs as string in tests.
type DebugLogger interface {
	Logger
	ConnectTo(writer io.Writer)
	Truncate()
	AllMessages() string
	DebugMessages() string
	InfoMessages() string
	WarnMessages() string
	WarnAndErrorMessages() string
	ErrorMessages() string

	AllMessagesTxt() string
}

type baseLogger interface {
	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)

	Debugf(template string, args ...any)
	Infof(template string, args ...any)
	Warnf(template string, args ...any)
	Errorf(template string, args ...any)

	Sync() error
}

type contextLogger interface {
	// Debug logs message in the debug level, you can use an attribute %placeholder% defined by the ctxattr package.
	Debug(ctx context.Context, message string)
	// Info logs message in the debug level, you can use an attribute %placeholder% defined by the ctxattr package.
	Info(ctx context.Context, message string)
	// Warn logs message in the debug level, you can use an attribute %placeholder% defined by the ctxattr package.
	Warn(ctx context.Context, message string)
	// Error logs message in the debug level, you can use an attribute %placeholder% defined by the ctxattr package.
	Error(ctx context.Context, message string)

	LogCtx(ctx context.Context, level string, args ...any)
	DebugCtx(ctx context.Context, args ...any)
	InfoCtx(ctx context.Context, args ...any)
	WarnCtx(ctx context.Context, args ...any)
	ErrorCtx(ctx context.Context, args ...any)

	DebugfCtx(ctx context.Context, template string, args ...any)
	InfofCtx(ctx context.Context, template string, args ...any)
	WarnfCtx(ctx context.Context, template string, args ...any)
	ErrorfCtx(ctx context.Context, template string, args ...any)

	Sync() error
}

type toWriter interface {
	DebugWriter() *LevelWriter
	InfoWriter() *LevelWriter
	WarnWriter() *LevelWriter
	ErrorWriter() *LevelWriter
}

type withFields interface {
	With(args ...any) Logger
	WithComponent(component string) Logger
}

type withPrefix interface {
	AddPrefix(prefix string) Logger
}
