package model

import (
	"time"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SliceWriting
// It is the initial state of the Slice.
// API node writes records to the local file.
const SliceWriting SliceState = "writing"

// SliceClosing
// Upload conditions have been met.
// Waiting for the API node until it switch to the new Slice.
const SliceClosing SliceState = "closing"

// SliceUploading
// The Slice is ready for upload.
// The worker from the same pod is/will be uploading the Slice.
const SliceUploading SliceState = "uploading"

// SliceUploaded
// The Slice has been successfully uploaded to the staging storage.
// The Slice can be removed from the local storage.
const SliceUploaded SliceState = "uploaded"

// SliceImported
//
// The parent File has been successfully imported to the target table.
// The Slice can be removed from the staging storage, if needed.
const SliceImported SliceState = "imported"

// SliceState is an enum type for slice states, see also FileState.
//
// Only following transitions are allowed:
// SliceWriting -> SliceClosing -> SliceUploading -> SliceUploaded -> SliceImported.
type SliceState string

func (s Slice) WithState(at time.Time, to SliceState) (Slice, error) {
	from := s.State
	atUTC := utctime.From(at)

	switch {
	case from == SliceWriting && to == SliceClosing:
		s.ClosingAt = &atUTC
	case from == SliceClosing && to == SliceUploading:
		s.UploadingAt = &atUTC
	case from == SliceUploading && to == SliceUploaded:
		s.UploadedAt = &atUTC
	case from == SliceUploaded && to == SliceImported:
		s.ImportedAt = &atUTC
	default:
		return Slice{}, serviceError.NewBadRequestError(errors.Errorf(`unexpected slice "%s" state transition from "%s" to "%s"`, s.SliceKey, from, to))
	}

	s.State = to
	s.ResetRetry()
	return s, nil
}

// Level gets the storage.Level at which the slice is present.
func (s SliceState) Level() Level {
	switch s {
	case SliceWriting, SliceClosing, SliceUploading:
		return LevelLocal
	case SliceUploaded:
		return LevelStaging
	case SliceImported:
		return LevelTarget
	default:
		panic(errors.Errorf(`unexpected slice state "%v"`, s))
	}
}
