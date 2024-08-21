package model

import (
	"time"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// FileWriting
// It is the initial state of the File.
// API nodes writes records to the File slices in the local volumes.
const FileWriting FileState = "writing"

// FileClosing
// Import conditions have been met.
// Waiting for all slices to be in the SliceUploaded state.
const FileClosing FileState = "closing"

// FileImporting
// The coordinator is/will be importing the File using a Storage Job.
const FileImporting FileState = "importing"

// FileImported
// The File has been successfully imported to the target table.
const FileImported FileState = "imported"

// FileState is an enum type for file states.
//
// Only following transitions are allowed:
// FileWriting -> FileClosing -> FileImporting -> FileImported
//
// Example File and Slice transitions.
//
//	    FILE            SLICE1           SLICE2           SLICE3
//	-----------------------------------------------------------------
//	FileWriting      SliceWriting     -------------    --------------
//	FileWriting      SliceClosing     SliceWriting     --------------
//	FileWriting      SliceUploading   SliceWriting     --------------
//	FileWriting      SliceUploaded    SliceWriting     --------------
//	FileWriting      SliceUploaded    SliceClosing     --------------
//	...
//	FileWriting      SliceUploaded    SliceUploaded    SliceWriting
//	FileClosing      SliceUploaded    SliceUploaded    SliceClosing
//	FileClosing      SliceUploaded    SliceUploaded    SliceUploading
//	FileClosing      SliceUploaded    SliceUploaded    SliceUploaded
//	FileImporting    SliceUploaded    SliceUploaded    SliceUploaded
//	FileImported     SliceImported    SliceImported    SliceImported
type FileState string

func (f FileState) String() string {
	return string(f)
}

func (f File) WithState(at time.Time, to FileState) (File, error) {
	from := f.State
	atUTC := utctime.From(at)

	switch {
	case from == FileWriting && to == FileClosing:
		f.ClosingAt = &atUTC
	case from == FileClosing && to == FileImporting:
		f.ImportingAt = &atUTC
	case from == FileImporting && to == FileImported:
		f.ImportedAt = &atUTC
	default:
		return File{}, serviceError.NewBadRequestError(errors.Errorf(`unexpected file "%s" state transition from "%s" to "%s"`, f.FileKey, from, to))
	}

	f.State = to
	f.ResetRetry()
	return f, nil
}

// Level gets the lowest storage.Level at which at least one file slice is present.
func (f FileState) Level() Level {
	switch f {
	case FileWriting, FileClosing:
		return LevelLocal
	case FileImporting:
		return LevelStaging
	case FileImported:
		return LevelTarget
	default:
		panic(errors.Errorf(`unexpected file state "%v"`, f))
	}
}
