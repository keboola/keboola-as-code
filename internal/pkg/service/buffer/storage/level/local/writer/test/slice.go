package test

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"time"
)

func NewSlice() *storage.Slice {
	return NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceOpenedAt(openedAt string) *storage.Slice {
	return &storage.Slice{
		SliceKey: storage.SliceKey{
			FileKey: storage.FileKey{
				ExportKey: key.ExportKey{
					ReceiverKey: key.ReceiverKey{
						ProjectID:  123,
						ReceiverID: "my-receiver",
					},
					ExportID: "my-export",
				},
				FileID: storage.FileID{
					OpenedAt: utctime.MustParse("2000-01-01T19:00:00.000Z"),
				},
			},
			SliceID: storage.SliceID{
				VolumeID: "my-volume",
				OpenedAt: utctime.MustParse(openedAt),
			},
		},
		Type:  storage.FileTypeCSV,
		State: storage.SliceWriting,
		Columns: column.Columns{
			column.ID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: local.Slice{
			Dir:           openedAt,
			Filename:      "slice.csv",
			AllocateSpace: 10 * datasize.KB,
			Compression: compression.Config{
				Type: compression.TypeNone,
			},
			Sync: disksync.Config{
				Mode:            disksync.ModeDisk,
				Wait:            true,
				CheckInterval:   1 * time.Millisecond,
				CountTrigger:    500,
				BytesTrigger:    1 * datasize.MB,
				IntervalTrigger: 50 * time.Millisecond,
			},
		},
		StagingStorage: staging.Slice{
			Path: "slice.csv",
			Compression: compression.Config{
				Type: compression.TypeNone,
			},
		},
	}
}
