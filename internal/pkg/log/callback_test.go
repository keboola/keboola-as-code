// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestCallbackLogger(t *testing.T) {
	t.Parallel()

	type record struct {
		entry  zapcore.Entry
		fields []zapcore.Field
	}
	records := make([]record, 0)
	logger := NewCallbackLogger(func(entry zapcore.Entry, fields []zapcore.Field) {
		records = append(records, record{entry: entry, fields: fields})
	})
	logger.Debug(`Debug message.`)
	logger.Info(`Info message.`)
	loggerWithFields := logger.With("key1", "value1", "key2", "value2")
	loggerWithFields.Debug(`Debug message.`)
	loggerWithFields.Info(`Info message.`)

	// Clear time
	for i, r := range records {
		r.entry.Time = time.Time{}
		records[i] = r
	}

	// Compare
	assert.Equal(t, []record{
		{entry: zapcore.Entry{Level: DebugLevel, Message: "Debug message."}},
		{entry: zapcore.Entry{Level: InfoLevel, Message: "Info message."}},
		{entry: zapcore.Entry{Level: DebugLevel, Message: "Debug message."}, fields: []zapcore.Field{
			{Key: "key1", Type: zapcore.StringType, String: "value1"},
			{Key: "key2", Type: zapcore.StringType, String: "value2"},
		}},
		{entry: zapcore.Entry{Level: InfoLevel, Message: "Info message."}, fields: []zapcore.Field{
			{Key: "key1", Type: zapcore.StringType, String: "value1"},
			{Key: "key2", Type: zapcore.StringType, String: "value2"},
		}},
	}, records)
}
