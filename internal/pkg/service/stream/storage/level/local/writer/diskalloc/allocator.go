// Package diskalloc provides allocation of a free disk space for a file.
// Allocation is realized using the UNIX syscall "fallocate" (POSIX syscall "fallocate" doesn't support FALLOC_FL_KEEP_SIZE).
//
// On systems other than Unix, this is implemented as no operation.
//
// Pre-allocation of free space causes the file to be one continuous block on the disk.
//
// It is crucial for the performance of HDD disks, where random access is slow (the read head must be moved).
// Even with SSD disks, this approach can bring some speed improvements, depending on the disk driver.
// Reading a continuous block during upload should be faster.
// Zero copy "sendfile" syscall is used for upload, so the data flows directly from the disk to the network.
package diskalloc

import (
	"github.com/c2h5oh/datasize"
)

type File interface {
	Fd() uintptr
}

type Allocator interface {
	Allocate(f File, size datasize.ByteSize) (bool, error)
}

// DefaultAllocator is default implementation of the Allocator interface.
type DefaultAllocator struct{}

func Default() Allocator {
	return &DefaultAllocator{}
}
