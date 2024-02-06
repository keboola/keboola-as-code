package staging

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Slice struct {
	// Path to the slice in the staging File.
	Path string `json:"path" validate:"required"`
	// Compression configuration.
	Compression compression.Config `json:"compression"  validate:"dive"`
}

func (f File) NewSlice(path string, localSlice local.Slice) (Slice, error) {
	// Add compression extension to the path
	switch f.Compression.Type {
	case compression.TypeNone:
		// nop
	case compression.TypeGZIP:
		path += ".gz"
	default:
		return Slice{}, errors.Errorf(`compression type "%s" is not supported by the staging storage`, f.Compression.Type)
	}

	return Slice{
		Path:        path,
		Compression: localSlice.Compression,
	}, nil
}
