package repository

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// newSlice creates slice definition.
func newSlice(now time.Time, file model.File, volumeID volume.ID, prevSliceSize datasize.ByteSize) (s model.Slice, err error) {
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
	sliceDir := filepath.FromSlash(sliceKey.SliceID.OpenedAt.String()) //nolint: forbidigo

	// Generate unique staging storage path
	stagingPath := fmt.Sprintf(`%s_%s`, sliceKey.OpenedAt().String(), sliceKey.VolumeID)

	s.SliceKey = sliceKey
	s.Type = file.Type
	s.State = model.SliceWriting
	s.Columns = file.Columns
	if s.LocalStorage, err = file.LocalStorage.NewSlice(sliceDir, prevSliceSize); err != nil {
		return model.Slice{}, err
	}
	if s.StagingStorage, err = file.StagingStorage.NewSlice(stagingPath, s.LocalStorage); err != nil {
		return model.Slice{}, err
	}

	return s, nil
}
