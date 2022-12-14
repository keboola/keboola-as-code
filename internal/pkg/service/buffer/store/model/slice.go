package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type Slice struct {
	key.SliceKey
	SliceNumber      int        `json:"sliceNumber" validate:"required"`
	ClosingAt        *time.Time `json:"closingAt,omitempty"`
	ClosedAt         *time.Time `json:"closedAt,omitempty"`
	UploadedAt       *time.Time `json:"uploadedAt,omitempty"`
	FailedAt         *time.Time `json:"failedAt,omitempty"`
	UploadStartedAt  *time.Time `json:"uploadStartedAt,omitempty"`
	UploadFinishedAt *time.Time `json:"uploadFinishedAt,omitempty"`
	LastError        string     `json:"lastError,omitempty"`
}

func NewSlice(fileKey key.FileKey, now time.Time, number int) Slice {
	return Slice{
		SliceKey:    key.SliceKey{FileKey: fileKey, SliceID: now},
		SliceNumber: number,
	}
}

func (v *Slice) OpenedAt() time.Time {
	return v.SliceID
}
