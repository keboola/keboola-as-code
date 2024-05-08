package test

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
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
		Type:    model.FileTypeCSV,
		State:   model.FileWriting,
		Columns: column.Columns{column.Body{}},
		Assignment: assignment.Assignment{
			Config: assignment.Config{
				Count:          1,
				PreferredTypes: []string{},
			},
			Volumes: []volume.ID{"my-volume"},
		},
		LocalStorage: local.File{
			Dir:         local.NormalizeDirPath(fileKey.String()),
			Compression: compression.NewNoneConfig(),
			DiskSync:    disksync.NewConfig(),
		},
		StagingStorage: staging.File{
			Compression: compression.NewNoneConfig(),
			Expiration:  utctime.From(openedAt.Time().Add(time.Hour)),
		},
		TargetStorage: target.Target{},
	}
}
