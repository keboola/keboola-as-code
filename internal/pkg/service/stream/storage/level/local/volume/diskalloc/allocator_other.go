//go:build !linux

package diskalloc

import (
	"github.com/c2h5oh/datasize"
)

func (a DefaultAllocator) Allocate(f File, size datasize.ByteSize) (bool, error) {
	// nop, Linux only
	return false, nil
}

func Allocated(path string) (datasize.ByteSize, error) {
	// nop, Linux only
	return 0, nil
}
