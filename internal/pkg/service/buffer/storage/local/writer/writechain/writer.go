package writechain

import (
	"io"
	"sync"
)

// Writer allows writing bytes or strings.
//
// Some writes are optimized for writing strings without an unnecessary conversion
// from []byte to string, so we support both methods.
//
// See Chain.PrependWriterOrErr method.
type Writer interface {
	io.Writer
	io.StringWriter
}

// stringWriterWrapper adds WriteString method to a writer without it.
type stringWriterWrapper struct {
	io.Writer
}

func (f *stringWriterWrapper) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func newStringWriterWrapper(w io.Writer) Writer {
	return &stringWriterWrapper{Writer: w}
}

// safeWriter add locks to an io.Writer.
// Write, WriteString and Flush are protected by the lock, because Flush is triggered asynchronously.
type safeWriter struct {
	originalWriter io.Writer
	stringWriter   Writer
	// lock synchronizes calls of the Write, WriteString, and Flush methods.
	lock *sync.Mutex
}

func newSafeWriter(w io.Writer) *safeWriter {
	// Add WriteString method, if it is not present
	var sw Writer
	if v, ok := w.(Writer); ok {
		sw = v
	} else {
		sw = newStringWriterWrapper(w)
	}

	// Setup Write/Flush lock
	return &safeWriter{
		originalWriter: w,
		stringWriter:   sw,
		lock:           &sync.Mutex{},
	}
}

func (w *safeWriter) Write(p []byte) (int, error) {
	w.lock.Lock()
	n, err := w.originalWriter.Write(p)
	w.lock.Unlock()
	return n, err
}

func (w *safeWriter) WriteString(s string) (int, error) {
	w.lock.Lock()
	n, err := w.stringWriter.WriteString(s)
	w.lock.Unlock()
	return n, err
}

func (w *safeWriter) Flush() (err error) {
	if v, ok := w.originalWriter.(flusher); ok {
		w.lock.Lock()
		err = v.Flush()
		w.lock.Unlock()
	}
	return err
}

func (w *safeWriter) String() string {
	return stringOrType(w.originalWriter)
}
