// nolint: forbidigo
package log

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToDebugWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.DebugWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "DEBUG  test\n", logger.AllMessages())
}

func TestToInfoWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.InfoWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "INFO  test\n", logger.AllMessages())
}

func TestToWarnWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.WarnWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "WARN  test\n", logger.AllMessages())
}

func TestToErrorWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.ErrorWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "ERROR  test\n", logger.AllMessages())
}

func TestWriteStringIndent(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.InfoWriter()
	writer.WriteStringIndent(1, "test1")
	writer.WriteStringIndent(2, "test2")
	writer.WriteStringIndent(3, "test3")
	expected := `
INFO    test1
INFO      test2
INFO        test3
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}
