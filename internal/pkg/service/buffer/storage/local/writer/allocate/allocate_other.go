//go:build !linux

package allocate

import (
	"github.com/c2h5oh/datasize"
	"os"
)

func (a DefaultAllocator) AllocateSpace(f *os.File, size datasize.ByteSize) (bool, error) {
	// nop, Linux only
	return false, nil
}

func AllocatedSize(path string) (datasize.ByteSize, error) {
	// nop, Linux only
	return 0, nil
}
