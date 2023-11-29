package test

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func NewFileKey() storage.FileKey {
	return NewFileKeyOpenedAt("2000-01-01T19:00:00.000Z")
}

func NewFileKeyOpenedAt(openedAtStr string) storage.FileKey {
	openedAt := utctime.MustParse(openedAtStr)
	return storage.FileKey{
		SinkKey: key.SinkKey{
			SourceKey: key.SourceKey{
				BranchKey: key.BranchKey{
					ProjectID: 123,
					BranchID:  456,
				},
				SourceID: "my-source",
			},
			SinkID: "my-sink",
		},
		FileID: storage.FileID{
			OpenedAt: openedAt,
		},
	}
}

func NewFile() *storage.File {
	return NewFileOpenedAt("2000-01-01T19:00:00.000Z")
}

func NewFileOpenedAt(openedAtStr string) *storage.File {
	openedAt := utctime.MustParse(openedAtStr)
	return &storage.File{
		FileKey: NewFileKeyOpenedAt(openedAtStr),
		Type:    storage.FileTypeCSV,
		State:   storage.FileWriting,
		Columns: column.Columns{column.Body{}},
		LocalStorage: local.File{
			Dir:         "my-dir",
			Compression: compression.DefaultNoneConfig(),
			DiskSync:    disksync.DefaultConfig(),
			VolumesAssignment: local.VolumesAssignment{
				PerPod:         1,
				PreferredTypes: []string{},
			},
		},
		StagingStorage: staging.File{
			Compression:                 compression.DefaultNoneConfig(),
			UploadCredentials:           &keboola.FileUploadCredentials{},
			UploadCredentialsExpiration: utctime.From(openedAt.Time().Add(time.Hour)),
		},
		TargetStorage: target.File{
			TableID:    keboola.MustParseTableID("in.bucket.table"),
			StorageJob: nil,
		},
	}
}
