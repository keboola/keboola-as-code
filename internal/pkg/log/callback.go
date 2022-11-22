// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"github.com/keboola/go-utils/pkg/deepcopy"
	"go.uber.org/zap/zapcore"
)

func NewCallbackLogger(fn callbackFn) Logger {
	return loggerFromZapCore(NewCallbackCore(fn))
}

func NewCallbackCore(fn callbackFn) zapcore.Core {
	return &callbackCore{callback: fn}
}

type callbackFn func(entry zapcore.Entry, fields []zapcore.Field)

type callbackCore struct {
	fields   []zapcore.Field
	callback callbackFn
}

// With creates a child core and adds structured context to it.
func (c *callbackCore) With(fields []zapcore.Field) zapcore.Core {
	// Return clone with added fields.
	return &callbackCore{
		fields:   append(deepcopy.Copy(c.fields).([]zapcore.Field), fields...),
		callback: c.callback,
	}
}

// Enabled for each level.
func (*callbackCore) Enabled(zapcore.Level) bool {
	return true
}

// Write log entry by callback.
func (c *callbackCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	c.callback(entry, append(c.fields, fields...))
	return nil
}

// Check - can this core log entry?
func (c *callbackCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(entry, c)
}

// Sync - nop.
func (*callbackCore) Sync() error {
	return nil
}
