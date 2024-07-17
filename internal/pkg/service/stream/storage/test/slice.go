package test

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
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
	encodingCfg := encoding.NewConfig()
	encodingCfg.Compression = compression.NewNoneConfig()
	encodingCfg.Sync = writesync.Config{
		Mode:                     writesync.ModeDisk,
		Wait:                     true,
		CheckInterval:            duration.From(1 * time.Millisecond),
		CountTrigger:             500,
		UncompressedBytesTrigger: 10 * datasize.MB,
		CompressedBytesTrigger:   1 * datasize.MB,
		IntervalTrigger:          duration.From(50 * time.Millisecond),
	}

	return &model.Slice{
		SliceKey: NewSliceKeyOpenedAt(openedAt),
		State:    model.SliceWriting,
		Mapping: table.Mapping{
			Columns: column.Columns{
				column.Datetime{Name: "datetime"},
				column.Body{Name: "body"},
			},
		},
		LocalStorage: localModel.Slice{
			Dir:                local.NormalizeDirPath(openedAt),
			Filename:           "slice.csv",
			AllocatedDiskSpace: 10 * datasize.KB,
			Encoding:           encodingCfg,
		},
		StagingStorage: stagingModel.Slice{
			Path:        "slice.csv",
			Compression: compression.NewNoneConfig(),
		},
	}
}
