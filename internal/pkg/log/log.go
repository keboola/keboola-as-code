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
}

// DebugLogger returns logs as string in tests.
type DebugLogger interface {
	Logger
	String() string
	Truncate()
	ConnectTo(writer io.Writer)
}

type baseLogger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Sync() error
}

type sugaredLogger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
}

type toWriter interface {
	DebugWriter() *LevelWriter
	InfoWriter() *LevelWriter
	WarnWriter() *LevelWriter
	ErrorWriter() *LevelWriter
}
