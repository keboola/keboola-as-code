package test

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func NewSliceKey() storage.SliceKey {
	return NewSliceKeyOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceKeyOpenedAt(openedAtStr string) storage.SliceKey {
	openedAt := utctime.MustParse(openedAtStr)
	return storage.SliceKey{
		FileKey: NewFileKeyOpenedAt("2000-01-01T19:00:00.000Z"),
		SliceID: storage.SliceID{
			VolumeID: "my-volume",
			OpenedAt: openedAt,
		},
	}
}

func NewSlice() *storage.Slice {
	return NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceOpenedAt(openedAt string) *storage.Slice {
	return &storage.Slice{
		SliceKey: NewSliceKeyOpenedAt(openedAt),
		Type:     storage.FileTypeCSV,
		State:    storage.SliceWriting,
		Columns: column.Columns{
			column.ID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: local.Slice{
			Dir:                openedAt,
			Filename:           "slice.csv",
			AllocatedDiskSpace: 10 * datasize.KB,
			Compression:        compression.DefaultNoneConfig(),
			DiskSync: disksync.Config{
				Mode:            disksync.ModeDisk,
				Wait:            true,
				CheckInterval:   1 * time.Millisecond,
				CountTrigger:    500,
				BytesTrigger:    1 * datasize.MB,
				IntervalTrigger: 50 * time.Millisecond,
			},
		},
		StagingStorage: staging.Slice{
			Path:        "slice.csv",
			Compression: compression.DefaultNoneConfig(),
		},
	}
}
