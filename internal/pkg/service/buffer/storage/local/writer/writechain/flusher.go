package writechain

type flusher interface {
	Flush() error
}

type flushFn func() error

func (v flushFn) Flush() error {
	return v()
}
