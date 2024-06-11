package limitbuffer

import (
	"io"
	"sync"
)

type Buffer struct {
	out     io.Writer
	maxSize int
	lock    sync.Mutex
	buffer  []byte
}

func New(out io.Writer, maxSize int) *Buffer {
	return &Buffer{out: out, maxSize: maxSize}
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	n = len(p)
	b.lock.Lock()

	// Flush, if there is no space or payload is too big
	l := len(b.buffer)
	if l+n > b.maxSize || n > b.maxSize/2 {
		// Skip buffer if the payload is too big and the buffer is empty
		if l == 0 {
			b.lock.Unlock()
			return b.out.Write(p)
		}

		// Flush buffer, if the payload is too big and the buffer is NOT empty
		if err = b.flush(); err != nil {
			b.lock.Unlock()
			return 0, err
		}
	}

	b.buffer = append(b.buffer, p...)
	b.lock.Unlock()

	return n, nil
}

func (b *Buffer) Flush() error {
	b.lock.Lock()
	err := b.flush()
	b.lock.Unlock()
	return err
}

func (b *Buffer) flush() error {
	_, err := b.out.Write(b.buffer)
	b.buffer = b.buffer[:0]
	return err
}
