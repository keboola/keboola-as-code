package model

import (
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

const (
	FileNameDateFormat = "20060102150405"
)

// File represent a file with records.
// A copy of the mapping is stored for retrieval optimization.
// On change in the Mapping, a new file and slice are created, so the Mapping field is immutable.
type File struct {
	key.FileKey
	State           filestate.State                `json:"state" validate:"required,oneof=opened closing importing imported failed"`
	Mapping         Mapping                        `json:"mapping" validate:"required,dive"`
	StorageResource *keboola.FileUploadCredentials `json:"storageResource" validate:"required"`
	ClosingAt       *utctime.UTCTime               `json:"closingAt,omitempty"`
	ImportingAt     *utctime.UTCTime               `json:"importingAt,omitempty"`
	ImportedAt      *utctime.UTCTime               `json:"importedAt,omitempty"`
	FailedAt        *utctime.UTCTime               `json:"failedAt,omitempty"`
	LastError       string                         `json:"lastError,omitempty"`
	RetryAttempt    int                            `json:"retryAttempt,omitempty"`
	RetryAfter      *utctime.UTCTime               `json:"retryAfter,omitempty"`
	IsEmpty         bool                           `json:"isEmpty,omitempty"`
	StorageJob      *keboola.StorageJob            `json:"storageJob,omitempty"`
}

func NewFile(exportKey key.ExportKey, now time.Time, mapping Mapping, resource *keboola.FileUploadCredentials) File {
	return File{
		FileKey:         key.FileKey{ExportKey: exportKey, FileID: key.FileID(now.UTC())},
		State:           filestate.Opened,
		Mapping:         mapping,
		StorageResource: resource,
	}
}

func (v *File) Filename() string {
	return fmt.Sprintf(`%s_%s_%s`, v.ReceiverID, v.ExportID, v.OpenedAt().Format(FileNameDateFormat))
}
