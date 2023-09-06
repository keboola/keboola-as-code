package writechain

type flusher interface {
	Flush() error
}

type flushFn struct {
	// info is a value used in the Chain.Dump, for example a related structure.
	info any
	fn   func() error
}

// newFlushFn allows to a custom function in the flusher interface.
// Info is a value used in the Chain.Dump, for example a related structure.
func newFlushFn(info any, fn func() error) flusher {
	return &flushFn{info: info, fn: fn}
}

func (v *flushFn) Flush() error {
	return v.fn()
}

func (v *flushFn) String() string {
	return stringOrType(v.info)
}
