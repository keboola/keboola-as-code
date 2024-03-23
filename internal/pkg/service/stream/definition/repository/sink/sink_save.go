package sink

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"
)

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Sink) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Do(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *definition.Sink) {
	// Call plugins
	r.plugins.Executor().OnSinkSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.WriteOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.SinkKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.SinkKey).Put(r.client, *updated),
		)
	} else {
		saveCtx.WriteOp(
			// Save record to the "active" prefix
			r.schema.Active().ByKey(updated.SinkKey).Put(r.client, *updated),
			// Save record to the versions history
			r.schema.Versions().Of(updated.SinkKey).Version(updated.VersionNumber()).Put(r.client, *updated),
		)

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(saveCtx.Now()) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.WriteOp(r.schema.Deleted().ByKey(updated.SinkKey).Delete(r.client))
		}
	}
}
