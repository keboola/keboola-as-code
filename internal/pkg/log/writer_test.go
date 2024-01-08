// nolint: forbidigo
package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToDebugWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.DebugWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	AssertJSONMessages(t, `{"level":"debug","message":"test"}`, logger.AllMessages())
}

func TestToInfoWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.InfoWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	AssertJSONMessages(t, `{"level":"info","message":"test"}`, logger.AllMessages())
}

func TestToWarnWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.WarnWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	AssertJSONMessages(t, `{"level":"warn","message":"test"}`, logger.AllMessages())
}

func TestToErrorWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.ErrorWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	AssertJSONMessages(t, `{"level":"error","message":"test"}`, logger.AllMessages())
}

func TestWriteStringIndent(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.InfoWriter()
	writer.WriteStringIndent(1, "test1")
	writer.WriteStringIndent(2, "test2")
	writer.WriteStringIndent(3, "test3")
	expected := `
{"level":"info","message":"  test1"}
{"level":"info","message":"    test2"}
{"level":"info","message":"      test3"}
`
	AssertJSONMessages(t, expected, logger.AllMessages())
}
