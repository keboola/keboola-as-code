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
	assert.Equal(t, "DEBUG  test\n", logger.String())
}

func TestToInfoWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.InfoWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "INFO  test\n", logger.String())
}

func TestToWarnWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.WarnWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "WARN  test\n", logger.String())
}

func TestToErrorWriter(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.ErrorWriter()
	_, err := writer.Write([]byte("test\n"))
	assert.NoError(t, err)
	assert.Equal(t, "ERROR  test\n", logger.String())
}

func TestWriteStringIndent(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	writer := logger.InfoWriter()
	writer.WriteStringIndent("test1", 1)
	writer.WriteStringIndent("test2", 2)
	writer.WriteStringIndent("test3", 3)
	expected := `
INFO    test1
INFO      test2
INFO        test3
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.String())
}
