//go:build !windows

package diskwriter

import (
	"github.com/ccoveille/go-safecast/v2"
	"golang.org/x/sys/unix"
)

func (v *Volume) UsedSpace() (uint64, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(v.Path(), &stat)
	if err != nil {
		return 0, err
	}

	blockSize, err := safecast.Convert[uint64](stat.Bsize)
	if err != nil {
		return 0, err
	}

	return (stat.Blocks - stat.Bavail) * blockSize, nil
}

func (v *Volume) TotalSpace() (uint64, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(v.Path(), &stat)
	if err != nil {
		return 0, err
	}

	blockSize, err := safecast.Convert[uint64](stat.Bsize)
	if err != nil {
		return 0, err
	}

	return stat.Blocks * blockSize, nil
}
