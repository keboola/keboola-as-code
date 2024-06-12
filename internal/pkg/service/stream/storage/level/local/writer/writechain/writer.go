package writechain

import (
	"io"
	"sync"
)

// safeWriter add locks to an io.Writer.
// Write, WriteString and Flush are protected by the lock, because Flush is triggered asynchronously.
type safeWriter struct {
	w io.Writer
	// lock synchronizes calls of the Write, WriteString, and Flush methods.
	lock *sync.Mutex
}

func newSafeWriter(w io.Writer) *safeWriter {
	// Setup Write/Flush lock
	return &safeWriter{
		w:    w,
		lock: &sync.Mutex{},
	}
}

func (w *safeWriter) Write(p []byte) (int, error) {
	w.lock.Lock()
	n, err := w.w.Write(p)
	w.lock.Unlock()
	return n, err
}

func (w *safeWriter) Flush() (err error) {
	if v, ok := w.w.(flusher); ok {
		w.lock.Lock()
		err = v.Flush()
		w.lock.Unlock()
	}
	return err
}

func (w *safeWriter) String() string {
	return stringOrType(w.w)
}
