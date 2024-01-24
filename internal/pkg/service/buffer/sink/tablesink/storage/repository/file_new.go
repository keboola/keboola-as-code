package repository

import (
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// newFile creates file entity.
func newFile(cfg tablesink.Config, resource FileResource, sink definition.Sink) (f storage.File, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch cfg.Storage.Local.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.File{}, errors.Errorf(`file compression type "%s" is not supported`, cfg.Storage.Local.Compression.Type)
	}

	// Convert path separator, on Windows
	fileDir := filepath.FromSlash(resource.FileKey.String()) //nolint:forbidigo

	f.FileKey = resource.FileKey
	f.Type = storage.FileTypeCSV // different file types are not supported now
	f.State = storage.FileWriting
	f.Columns = sink.Table.Mapping.Columns
	f.LocalStorage = local.NewFile(cfg.Storage.Local, fileDir)
	f.StagingStorage = staging.NewFile(cfg.Storage.Staging, f.LocalStorage, resource.Credentials)
	f.TargetStorage = target.NewFile(cfg.Storage.Target, sink.Table.Mapping.TableID, f.StagingStorage)
	f.Assignment.Config = cfg.Storage.VolumeAssignment

	return f, nil
}
