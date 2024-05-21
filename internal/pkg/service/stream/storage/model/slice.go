package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const sliceFilename = "slice"

// Slice represents a file slice prepared in the local storage to be uploaded into the staging storage.
// Slice is part of the File.
type Slice struct {
	SliceKey
	Retryable
	Deleted        bool             `json:"-"` // internal field to mark entity for deletion, there is no soft delete
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

// FileVolumeKey groups file slices at the same volume.
type FileVolumeKey struct {
	FileKey
	VolumeID model.ID `json:"volumeId" validate:"required"`
}

type SliceKey struct {
	FileVolumeKey
	SliceID
}

type SliceID struct {
	OpenedAt utctime.UTCTime `json:"sliceOpenedAt" validate:"required"`
}

func (v FileVolumeKey) String() string {
	if v.VolumeID == "" {
		panic(errors.New("storage.FileVolumeKey.ID cannot be empty"))
	}
	return v.FileKey.String() + "/" + v.VolumeID.String()
}

func (v SliceKey) String() string {
	return v.FileVolumeKey.String() + "/" + v.SliceID.String()
}

func (v SliceKey) OpenedAt() utctime.UTCTime {
	return v.SliceID.OpenedAt
}

func (v SliceID) String() string {
	if v.OpenedAt.IsZero() {
		panic(errors.New("storage.SliceID.OpenedAt cannot be empty"))
	}
	return v.OpenedAt.String()
}
