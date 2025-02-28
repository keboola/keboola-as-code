// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

func TestMemoryLogger(t *testing.T) {
	t.Parallel()

	mem := NewMemoryLogger()
	mem.Debug(t.Context(), `Debug message.`)
	mem.Info(t.Context(), `Info message.`)

	memWithAttrs := mem.
		WithComponent("c1").
		With(attribute.String("key1", "value1"), attribute.String("key2", "value2")).
		WithComponent("c2").
		With(attribute.String("key3", "value3")).
		WithDuration(123 * time.Second)

	ctx := ctxattr.ContextWith(t.Context(), attribute.String("key4", "value4"))
	memWithAttrs.Debug(ctx, `Debug message - <key1> <key2> <key3> <key4>`)
	memWithAttrs.Info(ctx, `Info message - <key1> <key2> <key3> <key4>`)

	target := NewDebugLogger()
	mem.CopyLogsTo(target)

	expected := `
{"level":"debug","message":"Debug message."}
{"level":"info","message":"Info message."}
{"level":"debug","message":"Debug message - value1 value2 value3 value4", "component":"c1.c2", "duration":"2m3s", "key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"}
{"level":"info","message":"Info message - value1 value2 value3 value4", "component":"c1.c2", "duration":"2m3s", "key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"}
`
	target.AssertJSONMessages(t, expected, target)
}
