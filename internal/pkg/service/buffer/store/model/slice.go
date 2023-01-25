package model

import (
	"time"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	SliceFilenameDateFormat = "20060102150405"
)

// Slice represent a file slice with records.
// A copy of the mapping is stored for retrieval optimization.
// A change in the mapping causes a new file and slice to be created so the mapping is immutable.
type Slice struct {
	key.SliceKey
	State           slicestate.State `json:"state" validate:"required,oneof=active/opened/writing active/opened/closing active/closed/uploading active/closed/uploaded active/closed/failed archived/successful/imported"`
	IsEmpty         bool             `json:"isEmpty,omitempty"`
	Mapping         Mapping          `json:"mapping" validate:"required,dive"`
	StorageResource *storageapi.File `json:"storageResource" validate:"required"`
	Number          int              `json:"sliceNumber" validate:"required"`
	ClosingAt       *UTCTime         `json:"closingAt,omitempty"`
	UploadingAt     *UTCTime         `json:"uploadingAt,omitempty"`
	UploadedAt      *UTCTime         `json:"uploadedAt,omitempty"`
	FailedAt        *UTCTime         `json:"failedAt,omitempty"`
	ImportedAt      *UTCTime         `json:"importedAt,omitempty"`
	LastError       string           `json:"lastError,omitempty"`
	RetryAttempt    int              `json:"retryAttempt,omitempty"`
	RetryAfter      *UTCTime         `json:"retryAfter,omitempty"`
	// Statistics are set by the "slice close" operation, the value is nil, if there is no record.
	Statistics *Stats        `json:"statistics,omitempty"`
	IDRange    *SliceIDRange `json:"idRange,omitempty"`
}

type SliceIDRange struct {
	Start uint64 `json:"start" validate:"required"`
	Count uint64 `json:"count" validate:"required"`
}

func NewSlice(fileKey key.FileKey, now time.Time, mapping Mapping, number int, resource *storageapi.File) Slice {
	return Slice{
		SliceKey:        key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(now)},
		State:           slicestate.Writing,
		Mapping:         mapping,
		StorageResource: resource,
		Number:          number,
	}
}

func (v Slice) Filename() string {
	return v.OpenedAt().Format(SliceFilenameDateFormat) + ".gz"
}

func (v Slice) GetStats() Stats {
	if v.State == slicestate.Writing || v.State == slicestate.Closing {
		panic(errors.Errorf(
			`slice "%s" in the state "%s" doesn't contain statistics, the state must be uploading, failed or uploaded`,
			v.SliceKey, v.State,
		))
	}
	// Statistics are not set for an empty slice.
	if v.Statistics == nil {
		return Stats{}
	}
	return *v.Statistics
}
