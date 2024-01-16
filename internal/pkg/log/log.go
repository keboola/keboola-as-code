package log

import (
	"context"
	"io"

	"github.com/stretchr/testify/assert"
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
	withFields
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

	CompareJSONMessages(expected string) error
	AssertJSONMessages(t assert.TestingT, expected string, msgAndArgs ...any) bool
}

type contextLogger interface {
	// Debug logs message in the debug level, you can use an attribute <placeholder> defined by the ctxattr package.
	Debug(ctx context.Context, message string)
	// Info logs message in the debug level, you can use an attribute <placeholder> defined by the ctxattr package.
	Info(ctx context.Context, message string)
	// Warn logs message in the debug level, you can use an attribute <placeholder> defined by the ctxattr package.
	Warn(ctx context.Context, message string)
	// Error logs message in the debug level, you can use an attribute <placeholder> defined by the ctxattr package.
	Error(ctx context.Context, message string)

	LogCtx(ctx context.Context, level string, args ...any)

	Debugf(ctx context.Context, template string, args ...any)
	Infof(ctx context.Context, template string, args ...any)
	Warnf(ctx context.Context, template string, args ...any)
	Errorf(ctx context.Context, template string, args ...any)

	Sync() error
}

type withFields interface {
	With(args ...any) Logger
	WithComponent(component string) Logger
}
