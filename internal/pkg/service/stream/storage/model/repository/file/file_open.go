package file

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (r *Repository) openFileOnSinkActivation() {
	r.plugins.Collection().OnSinkActivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if r.plugins.IsSinkWithLocalStorage(sink.Type) {
			op.AtomicOpFromCtx(ctx).AddFrom(r.openFileForSink(sink.SinkKey, now, plugin.SourceFromContext(ctx, sink.SourceKey), sink))
		}
		return nil
	})
}

// openFileForSink creates a new File in the FileWriting state, in the Sink.
func (r *Repository) openFileForSink(k key.SinkKey, now time.Time, source *definition.Source, sink *definition.Sink) *op.AtomicOp[model.File] {
	var file model.File
	atomicOp := op.Atomic(r.client, &file)

	source = r.loadSourceIfNil(atomicOp.Core(), k.SourceKey, source)

	sink = r.loadSinkIfNil(atomicOp.Core(), k, sink)

	// Open a new file
	atomicOp.Write(func(ctx context.Context) op.Op {
		// Apply configuration overrides from the source and the sink
		cfg := deepcopy.Copy(r.config).(level.Config)
		patch := level.ConfigPatch{}
		for _, kvs := range []configpatch.PatchKVs{source.Config, sink.Config} {
			err := configpatch.ApplyKVs(&cfg, &patch, kvs.In("storage.level"), configpatch.WithModifyProtected())
			if err != nil {
				return op.ErrorOp(err)
			}
		}

		// Create file entity
		var err error
		fileKey := model.FileKey{SinkKey: sink.SinkKey, FileID: model.FileID{OpenedAt: utctime.From(now)}}
		file, err = r.newFile(cfg, fileKey, *sink)
		if err != nil {
			return op.ErrorOp(err)
		}

		// Assign volumes
		file.Assignment = r.volumes.AssignVolumes(cfg.Local.Volume.Assignment, file.OpenedAt().Time())

		// At least one volume must be assigned
		if len(file.Assignment.Volumes) == 0 {
			return op.ErrorOp(errors.New(`no volume is available for the file`))
		}

		// Call plugins
		if err := r.plugins.Executor().OnFileOpen(ctx, now, *sink, &file); err != nil {
			return op.ErrorOp(err)
		}

		// Save new file
		return r.save(ctx, now, nil, &file)
	})

	return atomicOp
}
