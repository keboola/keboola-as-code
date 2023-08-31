package writechain

import (
	"io"
)

type closeFn struct {
	// info is a value used in the Chain.Dump, for example a related structure.
	info any
	fn   func() error
}

// newCloseFn allows to a custom function in the io.Closer interface.
// Info is a value used in the Chain.Dump, for example a related structure.
func newCloseFn(info any, fn func() error) io.Closer {
	return &closeFn{info: info, fn: fn}
}

func (v *closeFn) Close() error {
	return v.fn()
}

func (v *closeFn) String() string {
	return stringOrType(v.info)
}
