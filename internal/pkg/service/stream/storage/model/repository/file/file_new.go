package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// newFile creates file entity.
func (r *Repository) newFile(cfg level.Config, k model.FileKey, sink definition.Sink) (f model.File, err error) {
	if !r.plugins.IsSinkWithLocalStorage(sink.Type) {
		return model.File{}, errors.Errorf(`sink type "%s" has no local storage support`, sink.Type)
	}

	localDir := k.String()

	f.FileKey = k
	f.State = model.FileWriting
	f.Encoding = cfg.Local.Encoding
	f.Encoding.Compression = f.Encoding.Compression.Simplify()
	f.LocalStorage = local.NewFile(localDir, cfg.Local)
	f.StagingStorage = staging.NewFile(f.Encoding, k.OpenedAt().Time())
	f.TargetStorage = target.NewTarget()
	f.LocalStorage.Assignment.Config = cfg.Local.Volume.Assignment

	return f, nil
}
