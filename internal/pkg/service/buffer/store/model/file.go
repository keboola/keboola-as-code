package model

import (
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	ClosingAt       *UTCTime                       `json:"closingAt,omitempty"`
	ImportingAt     *UTCTime                       `json:"importingAt,omitempty"`
	ImportedAt      *UTCTime                       `json:"importedAt,omitempty"`
	FailedAt        *UTCTime                       `json:"failedAt,omitempty"`
	LastError       string                         `json:"lastError,omitempty"`
	RetryAttempt    int                            `json:"retryAttempt,omitempty"`
	RetryAfter      *UTCTime                       `json:"retryAfter,omitempty"`
	// Statistics are set by the "file close" operation, the value is nil, if there is no record.
	Statistics *Stats `json:"statistics,omitempty"`
	IsEmpty    bool   `json:"isEmpty,omitempty"`
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

func (v File) GetStats() Stats {
	if v.State == filestate.Opened || v.State == filestate.Closing {
		panic(errors.Errorf(
			`file "%s" in the state "%s" doesn't contain statistics, the state must be importing, failed or imported`,
			v.FileKey, v.State,
		))
	}
	// Statistics are not set for an empty file.
	if v.Statistics == nil {
		return Stats{}
	}
	return *v.Statistics
}
