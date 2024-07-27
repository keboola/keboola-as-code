package staging

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func NewFile(encodingCfg encoding.Config, slicesUploadCfg config.UploadConfig, openedAt time.Time) stagingModel.File {
	// Note: Compression in the staging storage is same as in the local storage, but it can be modified in the future.
	return stagingModel.File{
		Compression: encodingCfg.Compression,
		Upload:      slicesUploadCfg,
	}
}

func NewSlice(path string, f stagingModel.File) (stagingModel.Slice, error) {
	// Add compression extension to the path
	switch f.Compression.Type {
	case compression.TypeNone:
		// nop
	case compression.TypeGZIP:
		path += ".gz"
	default:
		return stagingModel.Slice{}, errors.Errorf(`compression type "%s" is not supported by the staging storage`, f.Compression.Type)
	}

	return stagingModel.Slice{
		Path:        path,
		Compression: f.Compression.Simplify(),
		Upload:      f.Upload,
	}, nil
}
