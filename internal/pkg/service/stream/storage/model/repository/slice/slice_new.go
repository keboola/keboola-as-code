package slice

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/file"
)

const (
	// DirPathSegments defines how many segments are in "<projectID>/<branchID>/<sourceID>/<sinkID>/<fileID>/<sliceID>".
	DirPathSegments = file.DirPathSegments + 1
)

// newSlice creates slice entity.
func (r *Repository) newSlice(now time.Time, file model.File, volumeID volume.ID) (s model.Slice, err error) {
	// Convert path separator, on Windows
	sliceKey := model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID},
		SliceID:       model.SliceID{OpenedAt: utctime.From(now)},
	}

	localDir := local.NormalizeDirPath(filepath.Join(file.LocalStorage.Dir, sliceKey.OpenedAt().String()))

	stagingPath := fmt.Sprintf(`%s_%s`, sliceKey.OpenedAt().String(), sliceKey.VolumeID)

	s.SliceKey = sliceKey
	s.State = model.SliceWriting
	s.Mapping = file.Mapping
	s.Encoding = file.Encoding
	if s.LocalStorage, err = local.NewSlice(localDir, file.Encoding.Compression); err != nil {
		return model.Slice{}, err
	}
	if s.StagingStorage, err = staging.NewSlice(stagingPath, file.StagingStorage); err != nil {
		return model.Slice{}, err
	}

	return s, nil
}
