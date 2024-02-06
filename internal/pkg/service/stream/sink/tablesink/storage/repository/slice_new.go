package repository

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// newSlice creates slice definition.
func newSlice(now time.Time, file storage.File, volumeID volume.ID, prevSliceSize datasize.ByteSize) (s storage.Slice, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch file.LocalStorage.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.Slice{}, errors.Errorf(`file compression type "%s" is not supported`, file.LocalStorage.Compression.Type)
	}

	// Convert path separator, on Windows
	sliceKey := storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID},
		SliceID:       storage.SliceID{OpenedAt: utctime.From(now)},
	}
	sliceDir := filepath.FromSlash(sliceKey.SliceID.OpenedAt.String()) //nolint: forbidigo

	// Generate unique staging storage path
	stagingPath := fmt.Sprintf(`%s_%s`, sliceKey.OpenedAt().String(), sliceKey.VolumeID)

	s.SliceKey = sliceKey
	s.Type = file.Type
	s.State = storage.SliceWriting
	s.Columns = file.Columns
	if s.LocalStorage, err = file.LocalStorage.NewSlice(sliceDir, prevSliceSize); err != nil {
		return storage.Slice{}, err
	}
	if s.StagingStorage, err = file.StagingStorage.NewSlice(stagingPath, s.LocalStorage); err != nil {
		return storage.Slice{}, err
	}

	return s, nil
}
