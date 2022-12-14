package model

import (
	"time"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

// File represent a file with records.
// A copy of the mapping is stored for retrieval optimization.
// A change in the mapping causes a new file to be created so the mapping is immutable.
type File struct {
	key.FileKey
	State              filestate.State  `json:"state" validate:"required,oneof=opened closing closed importing imported failed"`
	Mapping            Mapping          `json:"mapping" validate:"required,dive"`
	StorageResource    *storageapi.File `json:"storageResource" validate:"required"`
	ClosingAt          *time.Time       `json:"closingAt,omitempty"`
	ClosedAt           *time.Time       `json:"closedAt,omitempty"`
	ImportingAt        *time.Time       `json:"importingAt,omitempty"`
	ImportedAt         *time.Time       `json:"importedAt,omitempty"`
	ManifestUploadedAt *time.Time       `json:"manifestUploadedAt,omitempty"`
	FailedAt           *time.Time       `json:"failedAt,omitempty"`
	LastError          string           `json:"lastError,omitempty"`
}

func NewFile(exportKey key.ExportKey, now time.Time, mapping Mapping, resource *storageapi.File) File {
	return File{
		FileKey:         key.FileKey{ExportKey: exportKey, FileID: now},
		State:           filestate.Opened,
		Mapping:         mapping,
		StorageResource: resource,
	}
}

func (v *File) OpenedAt() time.Time {
	return v.FileID
}
