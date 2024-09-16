//go:build !windows

package telemetry

import (
	"os"
	"runtime"
	"syscall"
)

func UsedFileDescriptors() (int, error) {
	var directory string
	if runtime.GOOS == "darwin" {
		directory = "/dev/fd"
	} else {
		directory = "/proc/self/fd"
	}

	// nolint: forbidigo
	dir, err := os.Open(directory)
	if err != nil {
		return 0, err
	}
	defer dir.Close()

	files, err := dir.Readdirnames(-1)
	if err != nil {
		return 0, err
	}

	return len(files), nil
}

func TotalFileDescriptors() (uint64, error) {
	var limit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit)
	return limit.Cur, err
}
