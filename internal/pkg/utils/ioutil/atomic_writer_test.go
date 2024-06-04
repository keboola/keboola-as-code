package ioutil

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferedWriter(t *testing.T) {
	t.Parallel()
	writer := NewAtomicWriter()
	_, err := writer.WriteString("test")
	require.NoError(t, err)
	assert.Equal(t, "test", writer.String())
}

func TestBufferedWriter_ConnectTo(t *testing.T) {
	t.Parallel()
	writer := NewAtomicWriter()
	otherBuffer := bytes.Buffer{}
	otherWriter := bufio.NewWriter(&otherBuffer)
	writer.ConnectTo(otherWriter)

	_, err := writer.WriteString("test")
	require.NoError(t, err)
	require.NoError(t, otherWriter.Flush())

	assert.Equal(t, "test", writer.String())
	assert.Equal(t, "test", otherBuffer.String())
}
