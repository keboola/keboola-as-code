package log

import (
	"context"
	"io"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
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
	withAttributes
}

type LoggerWithZapCore interface {
	Logger
	ZapCore() zapcore.Core
}

// DebugLogger returns logs as string in tests.
type DebugLogger interface {
	Logger
	ConnectTo(writer io.Writer)
	ConnectInfoTo(writer io.Writer)
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
	// Debug logs message in the debug level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Debug(ctx context.Context, message string)
	// Info logs message in the info level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Info(ctx context.Context, message string)
	// Warn logs message in the warning level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Warn(ctx context.Context, message string)
	// Error logs message in the error level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Error(ctx context.Context, message string)
	// Log logs message in the level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Log(ctx context.Context, level string, message string)

	// Debugf logs formatted message in the debug level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Debugf(ctx context.Context, template string, args ...any)
	// Infof logs formatted message in the info level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Infof(ctx context.Context, template string, args ...any)
	// Warnf logs formatted message in the warning level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Warnf(ctx context.Context, template string, args ...any)
	// Errorf logs formatted message in the error level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Errorf(ctx context.Context, template string, args ...any)
	// Logf logs formatted message in the level, you can use an attribute <placeholder> for ctxattr or Logger.With attributes.
	Logf(ctx context.Context, level string, template string, args ...any)

	Sync() error
}

type withAttributes interface {
	With(attrs ...attribute.KeyValue) Logger
	WithComponent(component string) Logger
	WithDuration(v time.Duration) Logger
}
