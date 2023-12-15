package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// LevelLocal - data is buffered on a local disk.
	LevelLocal = Level("local")
	// LevelStaging - data is uploaded to the staging storage.
	LevelStaging = Level("staging")
	// LevelTarget - data is imported to the target storage.
	LevelTarget = Level("target")
)

// Level on which the data is stored during processing.
type Level string

func AllLevels() []Level {
	return []Level{LevelLocal, LevelStaging, LevelTarget}
}

func (l Level) String() string {
	return string(l)
}

// Level gets the lowest storage.Level at which at least one file slice is present.
func (s FileState) Level() Level {
	switch s {
	case FileWriting, FileClosing:
		return LevelLocal
	case FileImporting:
		return LevelStaging
	case FileImported:
		return LevelTarget
	default:
		panic(errors.Errorf(`unexpected file state "%v"`, s))
	}
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
