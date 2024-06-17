package slice

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// newSlice creates slice entity.
func (r *Repository) newSlice(now time.Time, file model.File, volumeID volume.ID) (s model.Slice, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch file.LocalStorage.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return model.Slice{}, errors.Errorf(`file compression type "%s" is not supported`, file.LocalStorage.Compression.Type)
	}

	// Convert path separator, on Windows
	sliceKey := model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID},
		SliceID:       model.SliceID{OpenedAt: utctime.From(now)},
	}

	localDir := sliceKey.OpenedAt().String()

	stagingPath := fmt.Sprintf(`%s_%s`, sliceKey.OpenedAt().String(), sliceKey.VolumeID)

	s.SliceKey = sliceKey
	s.Type = file.Type
	s.State = model.SliceWriting
	s.Columns = file.Columns
	if s.LocalStorage, err = local.NewSlice(localDir, file.LocalStorage); err != nil {
		return model.Slice{}, err
	}
	if s.StagingStorage, err = staging.NewSlice(stagingPath, file.StagingStorage, s.LocalStorage); err != nil {
		return model.Slice{}, err
	}

	return s, nil
}
