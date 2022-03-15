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
	return NewApiLogger(l.base, prefix, l.verbose)
}

// NewApiLogger new production zapLogger for API.
func NewApiLogger(base *stdLog.Logger, prefix string, verbose bool) PrefixLogger {
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
