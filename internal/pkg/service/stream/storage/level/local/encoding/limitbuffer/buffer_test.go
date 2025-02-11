package limitbuffer

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuffer_SmallChunks(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	maxSize := 10
	buf := New(&out, maxSize)

	// Test writing data smaller than maxSize/2
	data := []byte("12345")
	n, err := buf.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Fill the remaining space in the buffer
	data = []byte("67890")
	n, err = buf.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Check that buffer is not flushed yet
	assert.Equal(t, "", out.String())

	// Test writing data that causes flush
	data = []byte("0")
	n, err = buf.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Check if buffer was flushed correctly
	assert.Equal(t, "1234567890", out.String())
	require.NoError(t, buf.Flush())
	assert.Equal(t, "12345678900", out.String())
}

func TestBuffer_LargeChunk(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	maxSize := 10
	buf := New(&out, maxSize)

	// Test writing data larger than maxSize/2
	data := []byte("123456")
	n, err := buf.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Check that buffer was skipped
	assert.Equal(t, string(data), out.String())
}

func TestBuffer_MixedChunks(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	maxSize := 10
	buf := New(&out, maxSize)

	// Test writing data smaller than maxSize/2
	data := []byte("12345")
	n, err := buf.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Test writing data larger than maxSize/2
	data = []byte("6789012345")
	n, err = buf.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Check that buffer was skipped
	assert.Equal(t, "123456789012345", out.String())
}

func TestBuffer_ParallelUsage(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	maxSize := 10
	buf := New(&out, maxSize)

	var wg sync.WaitGroup
	numGoroutines := 10
	data := []byte("12345")

	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := buf.Write(data)
			require.NoError(t, err)
			assert.Equal(t, len(data), n)
		}()
	}

	wg.Wait()

	// Check if buffer was flushed correctly
	require.NoError(t, buf.Flush())
	expected := bytes.Repeat(data, numGoroutines)
	assert.Equal(t, string(expected), out.String())
}
