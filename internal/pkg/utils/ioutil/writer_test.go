package ioutil

import (
	"bufio"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/buffer"
)

func TestBufferedWriter(t *testing.T) {
	t.Parallel()
	writer := NewBufferedWriter()
	_, err := writer.WriteString("test")
	assert.NoError(t, err)
	assert.Equal(t, "test", writer.String())
}

func TestBufferedWriter_ConnectTo(t *testing.T) {
	t.Parallel()
	writer := NewBufferedWriter()
	otherBuffer := buffer.Buffer{}
	otherWriter := bufio.NewWriter(&otherBuffer)
	writer.ConnectTo(otherWriter)

	_, err := writer.WriteString("test")
	assert.NoError(t, err)
	assert.NoError(t, otherWriter.Flush())

	assert.Equal(t, "test", writer.String())
	assert.Equal(t, "test", otherBuffer.String())
}
