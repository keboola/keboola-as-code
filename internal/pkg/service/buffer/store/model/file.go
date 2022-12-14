package model

import (
	"time"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type File struct {
	key.FileKey
	Mapping            Mapping          `json:"mapping" validate:"required,dive"`
	StorageResource    *storageapi.File `json:"storageResource" validate:"required"`
	ClosingAt          *time.Time       `json:"closingAt,omitempty"`
	ClosedAt           *time.Time       `json:"closedAt,omitempty"`
	ManifestUploadedAt *time.Time       `json:"manifestUploadedAt,omitempty"`
	ImportedAt         *time.Time       `json:"importedAt,omitempty"`
	FailedAt           *time.Time       `json:"failedAt,omitempty"`
	ImportStartedAt    *time.Time       `json:"importStartedAt,omitempty"`
	ImportFinishedAt   *time.Time       `json:"importFinishedAt,omitempty"`
	LastError          string           `json:"lastError,omitempty"`
}

func NewFile(exportKey key.ExportKey, now time.Time, mapping Mapping, resource *storageapi.File) File {
	return File{
		FileKey:         key.FileKey{ExportKey: exportKey, FileID: now},
		Mapping:         mapping,
		StorageResource: resource,
	}
}

func (v *File) OpenedAt() time.Time {
	return v.FileID
}
