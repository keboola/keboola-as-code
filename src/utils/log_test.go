package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewBufferWriter(t *testing.T) {
	writer := NewBufferWriter()
	_, err := writer.WriteString("test")
	assert.NoError(t, err)
	err = writer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "test", writer.Buffer.String())
}

func TestNewDebugLogger(t *testing.T) {
	logger, writer := NewDebugLogger()
	logger.Warn("test")
	err := writer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, "WARN  test\n", writer.Buffer.String())
}
