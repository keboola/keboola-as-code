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
	// Debug, Info, Warn and Error accept a template string which can contain placeholders to be replaced by ctxattr attributes.
	// For example if the template contains "%status%" then this placeholder is replaced by an attribute with the key "status".
	Debug(ctx context.Context, template string)
	Info(ctx context.Context, template string)
	Warn(ctx context.Context, template string)
	Error(ctx context.Context, template string)

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

type withPrefix interface {
	AddPrefix(prefix string) Logger
}
