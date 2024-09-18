//go:build windows

package diskwriter

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

type diskUsage struct {
	freeBytes  int64
	totalBytes int64
	availBytes int64
}

func (v *Volume) UsedSpace() (uint64, error) {
	du, err := getDiskUsage(v.Path())

	return uint64(du.totalBytes - du.availBytes), err
}

func (v *Volume) TotalSpace() (uint64, error) {
	du, err := getDiskUsage(v.Path())

	return uint64(du.totalBytes), err
}

func getDiskUsage(path string) (diskUsage, error) {
	du := diskUsage{}

	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return du, err
	}

	dll, err := windows.LoadDLL("kernel32.dll")
	if err != nil {
		return du, err
	}

	proc, err := dll.FindProc("GetDiskFreeSpaceExW")
	if err != nil {
		return du, err
	}

	proc.Call(
		uintptr(unsafe.Pointer(pointer)),
		uintptr(unsafe.Pointer(&du.freeBytes)),
		uintptr(unsafe.Pointer(&du.totalBytes)),
		uintptr(unsafe.Pointer(&du.availBytes)),
	)

	return du, nil
}
