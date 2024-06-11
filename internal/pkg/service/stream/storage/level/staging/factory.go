package staging

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func NewFile(localFile localModel.File, openedAt time.Time) stagingModel.File {
	// Note: Compression in the staging storage is same as in the local storage, but it can be modified in the future.
	return stagingModel.File{
		Compression: localFile.Compression,
		Expiration:  utctime.From(openedAt.Add(stagingModel.DefaultFileExpiration)),
	}
}

func NewSlice(path string, f stagingModel.File, localSlice localModel.Slice) (stagingModel.Slice, error) {
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
		Compression: localSlice.Compression,
	}, nil
}
