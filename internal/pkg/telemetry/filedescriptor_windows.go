//go:build windows

package telemetry

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func UsedFileDescriptors() (int, error) {
	return 0, errors.New("file descriptors statistics not implemented for windows")
}

func TotalFileDescriptors() (uint64, error) {
	return 0, errors.New("file descriptors statistics not implemented for windows")
}
