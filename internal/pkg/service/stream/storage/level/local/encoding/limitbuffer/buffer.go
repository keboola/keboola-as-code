package

// Package limitbuffer provides a bytes buffer with limited maximum size.
// This prevents over-allocation of memory while waiting for compression or writing to disk.
// The size of the buffer is always doubled (Go append function) if it is full, up to the maximum size.
limitbuffer

import (
	"io"

	"github.com/sasha-s/go-deadlock"
)

type Buffer struct {
	out     io.Writer
	maxSize int
	lock    deadlock.Mutex
	buffer  []byte
}

func New(out io.Writer, maxSize int) *Buffer {
	// At the start, there is no allocated space.
	// It depends on the throughput, rate and size of messages, which the size of the buffer is sufficient.
	return &Buffer{out: out, maxSize: maxSize}
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	n = len(p)
	b.lock.Lock()

	// Flush, if there is no space or payload is too big
	largeChunk := n > b.maxSize/2
	l := len(b.buffer)
	if l > 0 && (largeChunk || l+n > b.maxSize) {
		// Flush buffer, if the payload is too big and the buffer is NOT empty
		if err = b.flush(); err != nil {
			b.lock.Unlock()
			return 0, err
		}
	}

	// Skip buffer if the payload is too big
	// The buffer is always empty at this point
	if largeChunk {
		b.lock.Unlock()
		return b.out.Write(p)
	}

	b.buffer = append(b.buffer, p...)
	b.lock.Unlock()

	return n, nil
}

func (b *Buffer) Flush() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.flush()
}

func (b *Buffer) flush() error {
	_, err := b.out.Write(b.buffer)
	b.buffer = b.buffer[:0]
	return err
}
