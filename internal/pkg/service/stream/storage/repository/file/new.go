package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// NewFile creates file entity.
func NewFile(cfg level.Config, k model.FileKey, sink definition.Sink) (f model.File, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch cfg.Local.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return model.File{}, errors.Errorf(`file compression type "%s" is not supported`, cfg.Local.Compression.Type)
	}

	localDir := k.String()

	f.FileKey = k
	f.Type = model.FileTypeCSV // different file types are not supported now
	f.State = model.FileWriting
	f.Columns = sink.Table.Mapping.Columns
	f.LocalStorage = local.NewFile(localDir, cfg.Local)
	f.StagingStorage = staging.NewFile(f.LocalStorage)
	f.TargetStorage = target.New(sink.Table.Keboola.TableID)
	f.Assignment.Config = cfg.Local.Volume.Assignment

	return f, nil
}
