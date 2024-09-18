//go:build !windows

package diskwriter

import "golang.org/x/sys/unix"

func (v *Volume) UsedSpace() (uint64, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(v.Path(), &stat)
	if err != nil {
		return 0, err
	}

	return (stat.Blocks - stat.Bavail) * uint64(stat.Bsize), nil
}

func (v *Volume) TotalSpace() (uint64, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(v.Path(), &stat)
	if err != nil {
		return 0, err
	}

	return stat.Blocks * uint64(stat.Bsize), nil
}
