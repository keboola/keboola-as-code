// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"github.com/keboola/go-utils/pkg/deepcopy"
	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func NewMemoryLogger() *MemoryLogger {
	var entries []memoryEntry
	core := &memoryCore{entries: &entries}
	return &MemoryLogger{
		zapLogger: newLoggerFromZapCore(core),
		core:      core,
	}
}

type MemoryLogger struct {
	*zapLogger
	core *memoryCore
}

func (l *MemoryLogger) CopyLogsTo(target Logger) {
	if zap, ok := target.(LoggerWithZapCore); ok {
		targetCore := zap.ZapCore()
		for _, entry := range *l.core.entries {
			if ce := targetCore.Check(entry.entry, nil); ce != nil {
				ce.Write(entry.fields...)
			}
		}
	} else {
		panic(errors.Errorf(`not implemented: cannot copy logs to "%T"`, target))
	}
}

type memoryCore struct {
	fields  []zapcore.Field
	entries *[]memoryEntry
}

type memoryEntry struct {
	entry  zapcore.Entry
	fields []zapcore.Field
}

// With creates a child core and adds structured context to it.
func (c *memoryCore) With(fields []zapcore.Field) zapcore.Core {
	// Return clone, but with the same entries slice.
	return &memoryCore{
		fields:  append(deepcopy.Copy(c.fields).([]zapcore.Field), fields...),
		entries: c.entries,
	}
}

// Enabled for each level.
func (*memoryCore) Enabled(zapcore.Level) bool {
	return true
}

// Write log entry to memory.
func (c *memoryCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	*c.entries = append(*c.entries, memoryEntry{
		entry:  entry,
		fields: append(c.fields, fields...), // merge logger level and entry level fields
	})
	return nil
}

// Check - can this core log entry?
func (c *memoryCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(entry, c)
}

// Sync - nop.
func (*memoryCore) Sync() error {
	return nil
}
