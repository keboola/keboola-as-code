// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap/zapcore"
)

// NewNopLogger returns no operation log. The logs are discarded.
func NewNopLogger() Logger {
	return loggerFromZapCore(zapcore.NewNopCore())
}
