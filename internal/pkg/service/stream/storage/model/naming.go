package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func SliceFilename(ft encoder.Type, ct compression.Type) (string, error) {
	filename := sliceFilename
	switch ft {
	case encoder.TypeCSV:
		filename += ".csv"
	default:
		return "", errors.Errorf(`unexpected encoder type "%s"`, ft)
	}

	return compression.Filename(filename, ct)
}
