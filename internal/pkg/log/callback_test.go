// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"context"
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
	logger.DebugCtx(context.Background(), `Debug message.`)
	logger.InfoCtx(context.Background(), `Info message.`)

	// Clear time
	for i, r := range records {
		r.entry.Time = time.Time{}
		records[i] = r
	}

	// Compare
	assert.Equal(t, []record{
		{entry: zapcore.Entry{Level: DebugLevel, Message: "Debug message."}},
		{entry: zapcore.Entry{Level: InfoLevel, Message: "Info message."}},
	}, records)
}
