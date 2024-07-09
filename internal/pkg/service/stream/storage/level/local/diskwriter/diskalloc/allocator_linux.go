//go:build linux

package diskalloc

import (
	"os"
	"syscall"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sys/unix"
)

func (a DefaultAllocator) Allocate(f File, size datasize.ByteSize) (bool, error) {
	// Allocate space using the "fallocate" sys call, Linux only.
	err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, int64(size.Bytes()))
	if err != nil {
		return false, err
	}
	return true, nil
}

func Allocated(path string) (datasize.ByteSize, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	sysStat, ok := stat.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, nil
	}

	// Notes:
	// 1. "/ 8": Blksize is in bits not bytes
	// 2. The type of fields can vary depending on the architecture (int32/int64), so retyping it to int64 is necessary.
	return datasize.ByteSize((int64(sysStat.Blksize / 8)) * int64(sysStat.Blocks)), nil // nolint:unconvert
}
