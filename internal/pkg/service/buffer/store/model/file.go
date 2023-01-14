package model

import (
	"time"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

// File represent a file with records.
// A copy of the mapping is stored for retrieval optimization.
// A change in the mapping causes a new file and slice to be created so the mapping is immutable.
type File struct {
	key.FileKey
	State              filestate.State  `json:"state" validate:"required,oneof=opened closing closed importing imported failed"`
	Mapping            Mapping          `json:"mapping" validate:"required,dive"`
	StorageResource    *storageapi.File `json:"storageResource" validate:"required"`
	ClosingAt          *UTCTime         `json:"closingAt,omitempty"`
	ClosedAt           *UTCTime         `json:"closedAt,omitempty"`
	ImportingAt        *UTCTime         `json:"importingAt,omitempty"`
	ImportedAt         *UTCTime         `json:"importedAt,omitempty"`
	ManifestUploadedAt *UTCTime         `json:"manifestUploadedAt,omitempty"`
	FailedAt           *UTCTime         `json:"failedAt,omitempty"`
	LastError          string           `json:"lastError,omitempty"`
}

func NewFile(exportKey key.ExportKey, now time.Time, mapping Mapping, resource *storageapi.File) File {
	return File{
		FileKey:         key.FileKey{ExportKey: exportKey, FileID: key.FileID(now.UTC())},
		State:           filestate.Opened,
		Mapping:         mapping,
		StorageResource: resource,
	}
}

func (v *File) OpenedAt() time.Time {
	return time.Time(v.FileID)
}
