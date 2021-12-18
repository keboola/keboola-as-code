// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap"
)

// NewNopLogger returns no operation log. The logs are discarded.
func NewNopLogger() Logger {
	return loggerFromZap(zap.NewNop())
}
