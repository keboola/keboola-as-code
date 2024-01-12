package repository

import (
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// newFile creates file entity.
func newFile(globalCfg storage.Config, resource FileResource, sink definition.Sink) (f storage.File, err error) {
	// File should be opened only for a table sink
	if sink.Type != definition.SinkTypeTable {
		panic(errors.New("file can be opened only for a table sink"))
	}

	// Apply configuration patch from the sink to the global config
	cfg := globalCfg.With(sink.Table.Storage)

	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch cfg.Local.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.File{}, errors.Errorf(`file compression type "%s" is not supported`, cfg.Local.Compression.Type)
	}

	// Convert path separator, on Windows
	fileDir := filepath.FromSlash(resource.FileKey.String()) //nolint:forbidigo

	f.FileKey = resource.FileKey
	f.Type = storage.FileTypeCSV // different file types are not supported now
	f.State = storage.FileWriting
	f.Columns = sink.Table.Mapping.Columns
	f.LocalStorage = local.NewFile(cfg.Local, fileDir)
	f.StagingStorage = staging.NewFile(cfg.Staging, f.LocalStorage, resource.Credentials)
	f.TargetStorage = target.NewFile(cfg.Target, sink.Table.Mapping.TableID, f.StagingStorage)

	return f, nil
}
