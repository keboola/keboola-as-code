// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryLogger(t *testing.T) {
	t.Parallel()

	mem := NewMemoryLogger()
	mem.Debug(`Debug message.`)
	mem.Info(`Info message.`)
	memWithCtx := mem.With("key1", "value1", "key2", "value2")
	memWithCtx.Debug(`Debug message.`)
	memWithCtx.Info(`Info message.`)

	target := NewDebugLogger()
	mem.CopyLogsTo(target)

	expected := `
DEBUG  Debug message.
INFO  Info message.
DEBUG  Debug message.  {"key1": "value1", "key2": "value2"}
INFO  Info message.  {"key1": "value1", "key2": "value2"}
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), target.AllMessages())
}
