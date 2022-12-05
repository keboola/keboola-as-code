package log

import (
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
	baseLogger
	sugaredLogger
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
	Log(level string, args ...any)
	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
	Sync() error
}

type sugaredLogger interface {
	With(args ...any) Logger // creates a child logger and adds structured context to it.
	Debugf(template string, args ...any)
	Infof(template string, args ...any)
	Warnf(template string, args ...any)
	Errorf(template string, args ...any)
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
