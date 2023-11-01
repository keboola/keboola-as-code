package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const sliceFilename = "slice"

// Slice represents a file slice prepared in the local storage to be uploaded into the staging storage.
// Slice is part of the File.
type Slice struct {
	SliceKey
	Retryable
	Type           FileType         `json:"type" validate:"required"`
	State          SliceState       `json:"state" validate:"required,oneof=writing closing uploading uploaded imported"`
	ClosingAt      *utctime.UTCTime `json:"closingAt,omitempty" validate:"excluded_if=State writing,required_if=State closing,required_if=State uploading,required_if=State uploaded,required_if=State imported"`
	UploadingAt    *utctime.UTCTime `json:"uploadingAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,required_if=State uploading,required_if=State uploaded,required_if=State imported"`
	UploadedAt     *utctime.UTCTime `json:"uploadedAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,excluded_if=State uploading,required_if=State uploaded,required_if=State imported"`
	ImportedAt     *utctime.UTCTime `json:"importedAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,excluded_if=State uploading,excluded_if=State uploaded,required_if=State imported"`
	Columns        column.Columns   `json:"columns" validate:"required,min=1"`
	LocalStorage   local.Slice      `json:"local"`
	StagingStorage staging.Slice    `json:"staging"`
}

type SliceKey struct {
	FileKey
	SliceID
}

type SliceID struct {
	VolumeID VolumeID        `json:"volumeId" validate:"required"`
	OpenedAt utctime.UTCTime `json:"openedAt" validate:"required"`
}

func (v SliceID) String() string {
	if v.OpenedAt.IsZero() {
		panic(errors.New("storage.SliceID.OpenedAt cannot be empty"))
	}
	return v.VolumeID.String() + "/" + v.OpenedAt.String()
}

func (v SliceKey) String() string {
	return v.FileKey.String() + "/" + v.SliceID.String()
}

func (v SliceKey) OpenedAt() utctime.UTCTime {
	return v.SliceID.OpenedAt
}
