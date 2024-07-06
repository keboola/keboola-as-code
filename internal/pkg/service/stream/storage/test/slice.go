package test

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func NewSliceKey() model.SliceKey {
	return NewSliceKeyOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceKeyOpenedAt(openedAtStr string) model.SliceKey {
	openedAt := utctime.MustParse(openedAtStr)
	return model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{
			FileKey:  NewFileKeyOpenedAt("2000-01-01T19:00:00.000Z"),
			VolumeID: "my-volume",
		},
		SliceID: model.SliceID{
			OpenedAt: openedAt,
		},
	}
}

func NewSlice() *model.Slice {
	return NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceOpenedAt(openedAt string) *model.Slice {
	return &model.Slice{
		SliceKey: NewSliceKeyOpenedAt(openedAt),
		Type:     model.FileTypeCSV,
		State:    model.SliceWriting,
		Columns: column.Columns{
			column.UUID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: localModel.Slice{
			Dir:                local.NormalizeDirPath(openedAt),
			Filename:           "slice.csv",
			AllocatedDiskSpace: 10 * datasize.KB,
			Compression:        compression.NewNoneConfig(),
			DiskSync: writesync.Config{
				Mode:                     writesync.ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(1 * time.Millisecond),
				CountTrigger:             500,
				UncompressedBytesTrigger: 10 * datasize.MB,
				CompressedBytesTrigger:   1 * datasize.MB,
				IntervalTrigger:          duration.From(50 * time.Millisecond),
			},
		},
		StagingStorage: stagingModel.Slice{
			Path:        "slice.csv",
			Compression: compression.NewNoneConfig(),
		},
	}
}
