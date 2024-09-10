package chunk_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/chunk"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestWriter_Empty(t *testing.T) {
	t.Parallel()

	// Create writer
	maxChunkSize := 10
	w := chunk.NewWriter(log.NewNopLogger(), maxChunkSize)

	// Flush
	assert.NoError(t, w.Flush())
	assert.Equal(t, 0, w.CompletedChunks())

	// Close
	assert.NoError(t, w.Close())
	assert.Equal(t, 0, w.CompletedChunks())

	// Compare
	var actualChunks []string
	assert.NoError(t, w.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
		actualChunks = append(actualChunks, string(chunk.Bytes()))
		return nil
	}))
	assert.Empty(t, actualChunks)
	assert.Equal(t, 0, w.CompletedChunks())
}

func TestWriter_Ok(t *testing.T) {
	t.Parallel()

	var expectedChunks []string

	// Create writer
	maxChunkSize := 10
	w := chunk.NewWriter(log.NewNopLogger(), maxChunkSize)

	// Write up to the maximum chunk size
	n, err := w.Write([]byte("12345"))
	assert.Equal(t, 5, n)
	assert.NoError(t, err)
	assert.Equal(t, 0, w.CompletedChunks())
	n, err = w.Write([]byte("67890"))
	assert.Equal(t, 5, n)
	assert.NoError(t, err)
	assert.Equal(t, 0, w.CompletedChunks())
	expectedChunks = append(expectedChunks, "aligned = false, data = 1234567890")

	// Write over the maximum
	n, err = w.Write([]byte("abc"))
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.Equal(t, 1, w.CompletedChunks())

	// Write over the maximum - split the payload
	n, err = w.Write([]byte("defghijkl"))
	assert.Equal(t, 9, n)
	assert.NoError(t, err)
	assert.Equal(t, 2, w.CompletedChunks())
	expectedChunks = append(expectedChunks, "aligned = false, data = abcdefghij")

	// Not empty flush
	assert.NoError(t, w.Flush())
	assert.Equal(t, 3, w.CompletedChunks())
	expectedChunks = append(expectedChunks, "aligned = true, data = kl")

	// Write long message, which requires more than 2 chunks
	n, err = w.Write([]byte("012345678901234567890123456789abc"))
	assert.Equal(t, 33, n)
	assert.NoError(t, err)
	assert.Equal(t, 6, w.CompletedChunks())
	expectedChunks = append(expectedChunks, "aligned = false, data = 0123456789")
	expectedChunks = append(expectedChunks, "aligned = false, data = 0123456789")
	expectedChunks = append(expectedChunks, "aligned = false, data = 0123456789")

	// Flush
	assert.NoError(t, w.Flush())
	assert.Equal(t, 7, w.CompletedChunks())
	expectedChunks = append(expectedChunks, "aligned = true, data = abc")

	// Empty flush
	assert.NoError(t, w.Flush())
	assert.Equal(t, 7, w.CompletedChunks())

	// Close
	n, err = w.Write([]byte("xyz"))
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	assert.Equal(t, 8, w.CompletedChunks())
	expectedChunks = append(expectedChunks, "aligned = true, data = xyz")

	// Compare
	var actualChunks []string
	assert.NoError(t, w.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
		actualChunks = append(actualChunks, fmt.Sprintf("aligned = %t, data = %s", chunk.Aligned(), chunk.Bytes()))
		return nil
	}))
	assert.Equal(t, expectedChunks, actualChunks)
	assert.Equal(t, 0, w.CompletedChunks())
}

func TestWriter_WaitForChunk(t *testing.T) {
	t.Parallel()

	// Create writer
	maxChunkSize := 10
	w := chunk.NewWriter(log.NewNopLogger(), maxChunkSize)

	// Get notifier
	notifier := w.WaitForChunkCh()
	select {
	case <-notifier:
		assert.Fail(t, "the channel should be blocked")
	default:
	}

	// A chunk is completed
	n, err := w.Write([]byte("abc"))
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Flush())
	assert.Equal(t, 1, w.CompletedChunks())

	// The notifier channel should be unblocked/closed by the first chunk
	select {
	case <-notifier:
	default:
		assert.Fail(t, "the channel shouldn't be blocked")
	}

	// There is an unprocessed complete chunk, the channel is not blocking
	select {
	case <-w.WaitForChunkCh():
	default:
		assert.Fail(t, "the channel shouldn't be blocked")
	}

	// Process all chunks
	assert.NoError(t, w.ProcessCompletedChunks(func(*chunk.Chunk) error {
		return nil
	}))

	// Wait for a next chunk
	notifier = w.WaitForChunkCh()
	select {
	case <-notifier:
		assert.Fail(t, "the channel should be blocked")
	default:
	}

	// The second chunk
	n, err = w.Write([]byte("def"))
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Flush())
	assert.Equal(t, 1, w.CompletedChunks())

	// The notifier channel should be unblocked/closed by the second chunk
	select {
	case <-notifier:
	default:
		assert.Fail(t, "the channel shouldn't be blocked")
	}

	// Close
	assert.NoError(t, w.Close())
	assert.Equal(t, 1, w.CompletedChunks())

	// Process all chunks
	assert.NoError(t, w.ProcessCompletedChunks(func(*chunk.Chunk) error {
		return nil
	}))
	assert.Equal(t, 0, w.CompletedChunks())

	// Writer is closed - the WaitForChunkCh doesn't block more
	assert.Equal(t, 0, w.CompletedChunks())
	for range 5 {
		select {
		case <-w.WaitForChunkCh():
		default:
			assert.Fail(t, "the channel shouldn't be blocked")
		}
	}
}

func TestWriter_ProcessCompletedChunks(t *testing.T) {
	t.Parallel()

	// Create writer
	maxChunkSize := 10
	w := chunk.NewWriter(log.NewNopLogger(), maxChunkSize)

	// There are 4 completed chunks
	n, err := w.Write([]byte("abc")) // 1
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Flush())
	n, err = w.Write([]byte("def")) // 2
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Flush())
	n, err = w.Write([]byte("ghi")) // 3
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Flush())
	n, err = w.Write([]byte("jkl")) // 4
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Flush())
	assert.Equal(t, 4, w.CompletedChunks()) // check

	// Get notifier
	notifier := w.WaitAllProcessedCh()
	select {
	case <-notifier:
		assert.Fail(t, "the channel should be blocked")
	default:
	}

	// No chunk is processed
	err = w.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
		return errors.New("error1")
	})
	if assert.Error(t, err) {
		assert.Equal(t, "error1", err.Error())
	}
	assert.Equal(t, 4, w.CompletedChunks())
	select {
	case <-notifier:
		assert.Fail(t, "the channel should be blocked")
	default:
	}

	// 1st chunk is processed
	index := 0
	err = w.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
		index++
		if index == 1 {
			assert.Equal(t, "abc", string(chunk.Bytes()))
			return nil
		}
		return errors.New("error2")
	})
	if assert.Error(t, err) {
		assert.Equal(t, "error2", err.Error())
	}
	assert.Equal(t, 3, w.CompletedChunks())
	select {
	case <-notifier:
		assert.Fail(t, "the channel should be blocked")
	default:
	}

	// 1st and 2nd chunk are processed
	index = 0
	err = w.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
		index++
		if index == 1 {
			assert.Equal(t, "def", string(chunk.Bytes()))
			return nil
		}
		if index == 2 {
			assert.Equal(t, "ghi", string(chunk.Bytes()))
			return nil
		}
		return errors.New("error3")
	})
	if assert.Error(t, err) {
		assert.Equal(t, "error3", err.Error())
	}
	assert.Equal(t, 1, w.CompletedChunks())
	select {
	case <-notifier:
		assert.Fail(t, "the channel should be blocked")
	default:
	}

	// Last chunk is processed
	assert.NoError(t, w.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
		assert.Equal(t, "jkl", string(chunk.Bytes()))
		return nil
	}))
	assert.Equal(t, 0, w.CompletedChunks())
	select {
	case <-notifier:
	default:
		assert.Fail(t, "the channel shouldn't be blocked")
	}

	// All chunks are processed, the WaitAllProcessedCh doesn't block
	select {
	case <-w.WaitAllProcessedCh():
	default:
		assert.Fail(t, "the channel shouldn't be blocked")
	}
}
