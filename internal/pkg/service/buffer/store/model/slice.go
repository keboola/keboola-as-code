package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
)

// Slice represent a file slice with records.
// A copy of the mapping is stored for retrieval optimization.
// A change in the mapping causes a new file and slice to be created so the mapping is immutable.
type Slice struct {
	key.SliceKey
	State       slicestate.State `json:"state" validate:"required,oneof=opened closing closed uploading uploaded failed"`
	Mapping     Mapping          `json:"mapping" validate:"required,dive"`
	Number      int              `json:"sliceNumber" validate:"required"`
	ClosingAt   *time.Time       `json:"closingAt,omitempty"`
	UploadingAt *time.Time       `json:"uploadingAt,omitempty"`
	UploadedAt  *time.Time       `json:"uploadedAt,omitempty"`
	FailedAt    *time.Time       `json:"failedAt,omitempty"`
	LastError   string           `json:"lastError,omitempty"`
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
