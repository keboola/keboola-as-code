package model

import (
	"fmt"
	"path/filepath"

	"github.com/c2h5oh/datasize"
)

type Slice struct {
	// Dir defines slice directory in the data volume.
	// The Dir also contains a lock and other auxiliary files.
	Dir string `json:"dir" validate:"required"`
	// FilenamePrefix is prefix of all slice partial files.
	FilenamePrefix string `json:"filenamePrefix" validate:"required"`
	// FilenameExtension is extension of all slice partial files.
	FilenameExtension string `json:"filenameExtension" validate:"required"`
	// IsEmpty is set if the upload was skipped because we did not receive any data.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// AllocatedDiskSpace defines the disk size that is pre-allocated when creating the slice.
	AllocatedDiskSpace datasize.ByteSize `json:"allocatedDiskSpace"`
}

func (s Slice) DirName(volumePath string) string {
	return filepath.Join(volumePath, s.Dir)
}

func (s Slice) FileName(volumePath string) string {
	return filepath.Join(s.DirName(volumePath), fmt.Sprintf("%s.%s", s.FilenamePrefix, s.FilenameExtension))
}
