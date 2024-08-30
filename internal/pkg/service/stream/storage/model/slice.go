package model

import (
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const sliceFilename = "slice"

// Slice represents a file slice prepared in the local storage to be uploaded into the staging storage.
// Slice is part of the File.
type Slice struct {
	SliceKey
	Retryable
	Deleted     bool             `json:"-"` // internal field to mark entity for deletion, there is no soft delete
	State       SliceState       `json:"state" validate:"required,oneof=writing closing uploading uploaded imported"`
	ClosingAt   *utctime.UTCTime `json:"closingAt,omitempty" validate:"excluded_if=State writing,required_if=State closing,required_if=State uploading,required_if=State uploaded,required_if=State imported"`
	UploadingAt *utctime.UTCTime `json:"uploadingAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,required_if=State uploading,required_if=State uploaded,required_if=State imported"`
	UploadedAt  *utctime.UTCTime `json:"uploadedAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,excluded_if=State uploading,required_if=State uploaded,required_if=State imported"`
	ImportedAt  *utctime.UTCTime `json:"importedAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,excluded_if=State uploading,excluded_if=State uploaded,required_if=State imported"`
	// Mapping defines how an incoming record is mapping to the result format, for example to a tabular data.
	Mapping table.Mapping `json:"mapping"` // in the future, here can be an interface - multiple mapping ways
	// Encoding defines how is the result format encoded, for example a tabular data to the CSV file.
	Encoding encoding.Config `json:"encoding"`
	// LocalStorage contains details of how the slice is stored in the local storage.
	LocalStorage localModel.Slice `json:"local"`
	// StagingStorage contains details of how the slice is uploaded to the staging storage.
	StagingStorage stagingModel.Slice `json:"staging"`
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

func (s Slice) LastStateChange() utctime.UTCTime {
	switch {
	case s.ClosingAt != nil:
		return *s.ClosingAt
	case s.UploadingAt != nil:
		return *s.UploadingAt
	case s.UploadedAt != nil:
		return *s.UploadedAt
	case s.ImportedAt != nil:
		return *s.ImportedAt
	default:
		return s.OpenedAt()
	}
}

func (s Slice) Telemetry() []attribute.KeyValue {
	lastStateChange := s.LastStateChange().Time()
	return []attribute.KeyValue{
		attribute.String("project.id", s.ProjectID.String()),
		attribute.String("branch.id", s.BranchID.String()),
		attribute.String("source.id", s.SourceID.String()),
		attribute.String("sink.id", s.SinkID.String()),
		attribute.String("file.id", s.FileID.String()),
		attribute.String("volume.id", s.VolumeID.String()),
		attribute.String("slice.id", s.SliceID.String()),
		attribute.String("slice.lastStateChange", lastStateChange.String()),
		attribute.Int("slice.retryAttempt", s.RetryAttempt),
	}
}
