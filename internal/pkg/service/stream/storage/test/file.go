package test

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	staging "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	target "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func NewFileKey() model.FileKey {
	return NewFileKeyOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileKeyOpenedAt(openedAtStr string) model.FileKey {
	openedAt := utctime.MustParse(openedAtStr)
	return model.FileKey{
		SinkKey: NewSinkKey(),
		FileID: model.FileID{
			OpenedAt: openedAt,
		},
	}
}

func NewFile() *model.File {
	return NewFileOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileOpenedAt(openedAtStr string) *model.File {
	fileKey := NewFileKeyOpenedAt(openedAtStr)
	return &model.File{
		FileKey: fileKey,
		State:   model.FileWriting,
		Mapping: table.Mapping{
			Columns: column.Columns{column.Body{Name: "body"}},
		},
		Encoding: encoding.NewConfig(),
		LocalStorage: localModel.File{
			Dir:        local.NormalizeDirPath(fileKey.String()),
			Allocation: diskalloc.NewConfig(),
			Assignment: assignment.Assignment{
				Config: assignment.Config{
					Count:          1,
					PreferredTypes: []string{},
				},
				Volumes: []volume.ID{"my-volume"},
			},
		},
		StagingStorage: stagingModel.File{
			Compression: compression.NewNoneConfig(),
			Upload: staging.UploadConfig{
				MinInterval: duration.From(1 * time.Second),
				Trigger: staging.UploadTrigger{
					Count:    10000,
					Size:     1 * datasize.MB,
					Interval: duration.From(1 * time.Minute),
				},
			},
		},
		TargetStorage: targetModel.Target{
			Import: target.ImportConfig{
				Trigger: target.ImportTrigger{
					Count:       50000,
					Size:        5 * datasize.MB,
					Interval:    duration.From(5 * time.Minute),
					SlicesCount: 100,
					Expiration:  duration.From(30 * time.Minute),
				},
			},
		},
	}
}
