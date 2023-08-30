package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	FileTypeCSV = "csv"
)

// File represents a file prepared in the staging storage to be imported into the target storage.
// File consists from zero or more Slices.
type File struct {
	FileKey
	Retryable
	Type           FileType         `json:"type" validate:"required,oneof=csv"`
	State          FileState        `json:"state" validate:"required,oneof=writing closing importing imported"`
	ClosingAt      *utctime.UTCTime `json:"closingAt,omitempty" validate:"excluded_if=State writing,required_if=State closing,required_if=State importing,required_if=State imported"`
	ImportingAt    *utctime.UTCTime `json:"importingAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,required_if=State importing,required_if=State imported"`
	ImportedAt     *utctime.UTCTime `json:"importedAt,omitempty"  validate:"excluded_if=State writing,excluded_if=State closing,excluded_if=State importing,required_if=State imported"`
	Columns        column.Columns   `json:"columns" validate:"required,min=1"`
	LocalStorage   local.File       `json:"local"`
	StagingStorage staging.File     `json:"staging"`
	TargetStorage  target.File      `json:"target"`
}

type FileType string

type FileKey struct {
	key.ExportKey
	FileID FileID `json:"fileId" validate:"required"`
}

type FileID struct {
	OpenedAt utctime.UTCTime `json:"openedAt" validate:"required"`
}

func (v FileID) String() string {
	if v.OpenedAt.IsZero() {
		panic(errors.New("storage.FileID.OpenedAt cannot be empty"))
	}
	return v.OpenedAt.String()
}

func (v FileKey) String() string {
	return v.ExportKey.String() + "/" + v.FileID.String()
}

func (v FileKey) OpenedAt() utctime.UTCTime {
	return v.FileID.OpenedAt
}
