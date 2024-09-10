package chunk

import (
	"context"
	"slices"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Writer splits written data to chunks, see ProcessCompletedChunks method.
type Writer struct {
	logger log.Logger
	lock   sync.Mutex
	// newChunkNotifier is closed when a chunk has been completed
	newChunkNotifier chan struct{}
	// allProcessedNotifier is closed when all chunks have been successfully processed.
	allProcessedNotifier chan struct{}
	// closed prevents Write/Flush methods calls after the Close method call.
	closed bool
	// maxChunkSize is a configured maximum chunk size, which is never exceeded.
	maxChunkSize int
	// maxChunkRealSize contains the maximum real size from all previous chunks.
	// It is used as initial buffer size, when creating a new chunk.
	maxChunkRealSize int
	// activeChunk receives new writes.
	activeChunk *Chunk
	// completedChunks from the oldest, to the newest.
	completedChunks []*Chunk
	// chunksPool reuses buffers from freed chunks.
	chunksPool *sync.Pool
}

func NewWriter(logger log.Logger, maxChunkSize int) *Writer {
	w := &Writer{}
	w.logger = logger.WithComponent("chunks")
	w.newChunkNotifier = make(chan struct{})
	w.allProcessedNotifier = make(chan struct{})
	w.maxChunkSize = maxChunkSize
	w.chunksPool = &sync.Pool{New: func() any { return newChunk(w.maxChunkRealSize) }}
	w.activeChunk = w.emptyChunk()
	return w
}

// Write data to the active chunk, overflow is written to a new chunk.
func (w *Writer) Write(p []byte) (total int, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.closed {
		return 0, errors.New("chunk.Writer is closed")
	}

	toWrite := p
	for len(toWrite) > 0 {
		// Shortcut: write to the active chunk, if there is a free space
		activeChunkSize := w.activeChunk.buffer.Len()
		if activeChunkSize+len(toWrite) <= w.maxChunkSize {
			n, err := w.activeChunk.write(toWrite)
			if err != nil {
				return total, err
			}
			total += n
			break
		}

		// Get remaining space in the active chunk
		remainingSpace := w.maxChunkSize - activeChunkSize

		// Determine how big part of the payload fits to the active chunk
		toActual := toWrite[:remainingSpace]

		// Determine how big part overflows to the next chunk
		toWrite = toWrite[remainingSpace:]
		nextChunkSize := min(len(toWrite), w.maxChunkSize)

		// Write overflow bytes to the new chunk
		// This is before the code block bellow, because on error, we can throw the new chunk away.
		next := w.emptyChunk()
		{
			n, err := next.write(toWrite[:nextChunkSize])
			if err != nil {
				w.freeChunk(next)
				return total, err
			}
			total += n
			toWrite = toWrite[nextChunkSize:]
		}

		// Write bytes to the active chunk, up to the max chunk size, if there is some space
		if len(toActual) > 0 {
			n, err := w.activeChunk.write(toActual)
			if err != nil {
				w.freeChunk(next)
				return total, err
			}
			total += n
		}

		// Actual chunk is full, swap
		w.swapChunks(next)
	}

	return total, nil
}

// Flush operation closes the active chunk with Aligned flag set to true.
// A new chunk is opened.
func (w *Writer) Flush() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.closed {
		return errors.New("chunk.Writer is closed")
	}

	// Swap chunks
	w.activeChunk.aligned = true
	w.swapChunks(w.emptyChunk())
	return nil
}

// Close operation closes the active chunk with Aligned flag set to true.
// No new chunk is created, future writes are forbidden.
func (w *Writer) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.closed {
		return errors.New("chunk.Writer is closed")
	}

	// Complete the active chunk
	w.activeChunk.aligned = true
	w.swapChunks(nil)

	w.closed = true
	close(w.newChunkNotifier)
	return nil
}

// WaitForChunkCh waits until a next chunk is completed.
// If the writer is closed or there is some chunk for the processing,
// a closed (not blocking) channel is returned.
func (w *Writer) WaitForChunkCh() <-chan struct{} {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.closed || len(w.completedChunks) > 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return w.newChunkNotifier
}

// WaitAllProcessedCh waits until count of unprocessed chunks is zero.
func (w *Writer) WaitAllProcessedCh() <-chan struct{} {
	w.lock.Lock()
	defer w.lock.Unlock()
	if len(w.completedChunks) == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return w.allProcessedNotifier
}

// CompletedChunks count.
func (w *Writer) CompletedChunks() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return len(w.completedChunks)
}

// ProcessCompletedChunks iterates over completed chunks.
// The method can be used, for example, to send/upload chunks to the next stage.
// If the callback is successful, the chunk is removed from the list and the internal buffer is reused.
func (w *Writer) ProcessCompletedChunks(fn func(chunk *Chunk) error) error {
	// Remove processed chunks at the end
	var processedIndex int
	defer func() {
		w.lock.Lock()
		defer w.lock.Unlock()

		w.completedChunks = w.completedChunks[processedIndex:]
		if len(w.completedChunks) == 0 {
			close(w.allProcessedNotifier)
			w.allProcessedNotifier = make(chan struct{})
		}
	}()

	// Clone slice to release the lock
	w.lock.Lock()
	chunks := slices.Clone(w.completedChunks)
	w.lock.Unlock()

	// Call function for each chunk, stop on error
	for _, chunk := range chunks {
		if err := fn(chunk); err != nil {
			return err
		}
		processedIndex++
		w.freeChunk(chunk)
	}

	w.logger.Debugf(context.Background(), "%d chunks written", processedIndex)
	return nil
}

// freeChunk after it is no longer used.
func (w *Writer) freeChunk(v *Chunk) {
	w.chunksPool.Put(v)
}

func (w *Writer) swapChunks(newChunk *Chunk) {
	if l := w.activeChunk.buffer.Len(); l > 0 {
		w.maxChunkRealSize = min(w.maxChunkRealSize, l)
		w.completedChunks = append(w.completedChunks, w.activeChunk)
		w.logger.Debugf(context.Background(), "chunk completed, aligned = %t, size = %q", w.activeChunk.Aligned(), datasize.ByteSize(l).String())

		// Unblock WaitForChunkCh method
		close(w.newChunkNotifier)
		w.newChunkNotifier = make(chan struct{})
	} else {
		w.freeChunk(w.activeChunk)
	}
	w.activeChunk = newChunk
}

func (w *Writer) emptyChunk() *Chunk {
	c := w.chunksPool.Get().(*Chunk)
	c.reset()
	return c
}
