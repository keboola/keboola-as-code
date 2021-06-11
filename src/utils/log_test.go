package utils

import (
	"bufio"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/buffer"
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

func TestConnectTo(t *testing.T) {
	writer := NewBufferWriter()
	otherBuffer := buffer.Buffer{}
	otherWriter := bufio.NewWriter(&otherBuffer)
	writer.ConnectTo(otherWriter)

	_, err := writer.WriteString("test")
	assert.NoError(t, err)
	assert.NoError(t, otherWriter.Flush())

	assert.Equal(t, "test", writer.String())
	assert.Equal(t, "test", otherBuffer.String())
}
