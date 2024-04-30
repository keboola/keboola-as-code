package file

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (r *Repository) openFileOnSinkActivation() {
	r.plugins.Collection().OnSinkActivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) {
		// Check is the sink type has support for files
		if !r.isSinkWithLocalStorage(sink) {
			return
		}

		// Open a new file
		op.AtomicFromCtx(ctx).AddFrom(r.openSink(now, plugin.SourceFromContext(ctx), *sink))
	})
}

func (r *Repository) openSink(now time.Time, source *definition.Source, sink definition.Sink) *op.AtomicOp[model.File] {
	var newFile model.File
	atomicOp := op.Atomic(r.client, &newFile)

	// Load Source entity, if needed
	if source == nil {
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.definition.Source().Get(sink.SourceKey).WithResultTo(source)
		})
	}

	// Open a new file
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		// Apply configuration overrides from the source and the sink
		cfg := r.config
		patch := level.ConfigPatch{}
		for _, kvs := range []configpatch.PatchKVs{source.Config, sink.Config} {
			err := configpatch.ApplyKVs(&cfg, &patch, kvs.In("storage.level"), configpatch.WithModifyProtected())
			if err != nil {
				return nil, err
			}
		}

		// Create file entity
		var err error
		fileKey := model.FileKey{SinkKey: sink.SinkKey, FileID: model.FileID{OpenedAt: utctime.From(now)}}
		newFile, err = NewFile(cfg, fileKey, sink)
		if err != nil {
			return nil, err
		}

		// Assign volumes
		newFile.Assignment = r.volumes.AssignVolumes(cfg.Local.Volume.Assignment, newFile.OpenedAt().Time())

		// At least one volume must be assigned
		if len(newFile.Assignment.Volumes) == 0 {
			return nil, errors.New(`no volume is available for the file`)
		}

		// Save new file
		return r.save(ctx, now, nil, &newFile), nil
	})

	return atomicOp
}
