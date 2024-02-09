// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestMemoryLogger(t *testing.T) {
	t.Parallel()

	mem := NewMemoryLogger()
	mem.Debug(context.Background(), `Debug message.`)
	mem.Info(context.Background(), `Info message.`)

	memWithCtx := mem.
		WithComponent("c1").
		With(attribute.String("key1", "value1"), attribute.String("key2", "value2")).
		WithComponent("c2").
		With(attribute.String("key3", "value3")).
		WithDuration(123 * time.Second)

	memWithCtx.Debug(context.Background(), `Debug message.`)
	memWithCtx.Info(context.Background(), `Info message.`)

	target := NewDebugLogger()
	mem.CopyLogsTo(target)

	expected := `
{"level":"debug","message":"Debug message."}
{"level":"info","message":"Info message."}
{"level":"debug","message":"Debug message.", "component":"c1.c2", "duration":"2m3s", "key1": "value1", "key2": "value2", "key3": "value3"}
{"level":"info","message":"Info message.", "component":"c1.c2", "duration":"2m3s", "key1": "value1", "key2": "value2", "key3": "value3"}
`
	target.AssertJSONMessages(t, expected, target)
}
