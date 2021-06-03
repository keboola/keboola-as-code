package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewBufferWriter(t *testing.T) {
	writer, buffer := NewBufferWriter()
	_, err := writer.WriteString("test")
	assert.NoError(t, err)
	err = writer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "test", buffer.String())
}

func TestNewDebugLogger(t *testing.T) {
	logger, writer, buffer := NewDebugLogger()
	logger.Warn("test")
	err := writer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "WARN  test\n", buffer.String())
}
