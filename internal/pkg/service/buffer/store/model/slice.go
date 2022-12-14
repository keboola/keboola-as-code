package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
)

type Slice struct {
	key.SliceKey
	State       slicestate.State `json:"state" validate:"required,oneof=opened closing closed uploading uploaded failed"`
	Number      int              `json:"sliceNumber" validate:"required"`
	ClosingAt   *time.Time       `json:"closingAt,omitempty"`
	ClosedAt    *time.Time       `json:"closedAt,omitempty"`
	UploadingAt *time.Time       `json:"uploadingAt,omitempty"`
	UploadedAt  *time.Time       `json:"uploadedAt,omitempty"`
	FailedAt    *time.Time       `json:"failedAt,omitempty"`
	LastError   string           `json:"lastError,omitempty"`
}

func NewSlice(fileKey key.FileKey, now time.Time, number int) Slice {
	return Slice{
		SliceKey: key.SliceKey{FileKey: fileKey, SliceID: now},
		State:    slicestate.Opened,
		Number:   number,
	}
}

func (v *Slice) OpenedAt() time.Time {
	return v.SliceID
}
