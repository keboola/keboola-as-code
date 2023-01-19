package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
)

const (
	SliceFilenameDateFormat = "20060102150405"
)

// Slice represent a file slice with records.
// A copy of the mapping is stored for retrieval optimization.
// A change in the mapping causes a new file and slice to be created so the mapping is immutable.
type Slice struct {
	key.SliceKey
	State       slicestate.State `json:"state" validate:"required,oneof=opened closing closed uploading uploaded failed"`
	IsEmpty     bool             `json:"isEmpty,omitempty"`
	Mapping     Mapping          `json:"mapping" validate:"required,dive"`
	Number      int              `json:"sliceNumber" validate:"required"`
	ClosingAt   *UTCTime         `json:"closingAt,omitempty"`
	UploadingAt *UTCTime         `json:"uploadingAt,omitempty"`
	UploadedAt  *UTCTime         `json:"uploadedAt,omitempty"`
	FailedAt    *UTCTime         `json:"failedAt,omitempty"`
	LastError   string           `json:"lastError,omitempty"`
	Attempt     int              `json:"attempt,omitempty"`
	// Statistics are set by the "slice close" operation, the value is nil, if there is no record.
	Statistics *Stats        `json:"statistics,omitempty"`
	IDRange    *SliceIDRange `json:"idRange,omitempty"`
}

type SliceIDRange struct {
	Start uint64 `json:"start" validate:"required"`
	Count uint64 `json:"count" validate:"required"`
}

func NewSlice(fileKey key.FileKey, now time.Time, mapping Mapping, number int) Slice {
	return Slice{
		SliceKey: key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(now)},
		State:    slicestate.Opened,
		Mapping:  mapping,
		Number:   number,
	}
}

func (v *Slice) OpenedAt() time.Time {
	return time.Time(v.SliceID)
}

func (v *Slice) Filename() string {
	return v.OpenedAt().Format(SliceFilenameDateFormat)
}
