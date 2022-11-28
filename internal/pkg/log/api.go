// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	stdLog "log"

	"go.uber.org/zap/zapcore"
)

type apiLogger struct {
	Logger
	base    *stdLog.Logger
	prefix  string
	verbose bool
}

func (l *apiLogger) Prefix() string {
	return l.prefix
}

// WithPrefix returns a new logger with different prefix.
func (l *apiLogger) WithPrefix(prefix string) PrefixLogger {
	return NewAPILogger(l.base, prefix, l.verbose)
}

// WithAdditionalPrefix returns a new logger with the added prefix.
func (l *apiLogger) WithAdditionalPrefix(prefix string) PrefixLogger {
	return NewAPILogger(l.base, l.prefix+prefix, l.verbose)
}

// NewAPILogger new production zapLogger for API.
func NewAPILogger(base *stdLog.Logger, prefix string, verbose bool) PrefixLogger {
	var cores []zapcore.Core

	// Log to the standard logger
	cores = append(cores, stdCore(base, prefix, verbose))

	// Create zapLogger
	return &apiLogger{
		Logger:  loggerFromZapCore(zapcore.NewTee(cores...)),
		base:    base,
		prefix:  prefix,
		verbose: verbose,
	}
}
