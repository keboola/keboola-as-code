package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func SliceFilename(ft FileType, ct compression.Type) (string, error) {
	filename := sliceFilename
	switch ft {
	case FileTypeCSV:
		filename += ".csv"
	default:
		return "", errors.Errorf(`unexpected file type "%s"`, ft)
	}

	return compression.Filename(filename, ct)
}
