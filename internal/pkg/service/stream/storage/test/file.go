package test

import (
	"time"

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
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
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

func NewFile() model.File {
	return NewFileOpenedAt("2000-01-01T01:00:00.000Z")
}

func NewFileOpenedAt(openedAtStr string) model.File {
	openedAt := utctime.MustParse(openedAtStr)
	fileKey := NewFileKeyOpenedAt(openedAtStr)
	return model.File{
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
			Expiration:  utctime.From(openedAt.Time().Add(time.Hour)),
		},
		TargetStorage: targetModel.Target{},
	}
}
