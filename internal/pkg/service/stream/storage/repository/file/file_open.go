package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (r *Repository) openFileOnSinkActivation() {
	r.plugins.Collection().OnSinkActivation(func(ctx *plugin.SaveContext, old, sink *definition.Sink) {
		r.open(ctx, source, *sink, volumes)
	})
}

func (r *Repository) open(ctx *plugin.SaveContext, source definition.Source, sink definition.Sink, volumes []volume.Metadata) (model.File, error) {
	// Apply configuration overrides from the source and the sink
	cfg := r.config
	patch := level.ConfigPatch{}
	for _, kvs := range []configpatch.PatchKVs{source.Config, sink.Config} {
		err := configpatch.ApplyKVs(&cfg, &patch, kvs.In("storage.level"), configpatch.WithModifyProtected())
		if err != nil {
			return model.File{}, err
		}
	}

	// Create file entity
	fileKey := model.FileKey{SinkKey: sink.SinkKey, FileID: model.FileID{OpenedAt: utctime.From(ctx.Now())}}
	newFile, err := NewFile(cfg, fileKey, sink)
	if err != nil {
		return model.File{}, err
	}

	// Assign volumes
	newFile.Assignment = r.volumes.AssignVolumes(volumes, cfg.Local.Volume.Assignment, newFile.OpenedAt().Time())

	// At least one volume must be assigned
	if len(newFile.Assignment.Volumes) == 0 {
		return model.File{}, errors.New(`no volume is available for the file`)
	}

	// Save new file
	r.save(ctx, nil, &newFile)

	return newFile, nil
}
