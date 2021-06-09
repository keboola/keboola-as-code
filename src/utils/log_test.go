package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewBufferWriter(t *testing.T) {
	writer := NewBufferWriter()
	_, err := writer.WriteString("test")
	assert.NoError(t, err)
	assert.Equal(t, "test", writer.String())
}

func TestNewDebugLogger(t *testing.T) {
	logger, writer := NewDebugLogger()
	logger.Warn("test")
	assert.Equal(t, "WARN  test\n", writer.String())
}
