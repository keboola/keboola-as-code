package branch

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"
)

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Branch) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, now, old, updated)
	return saveCtx.Do(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, now time.Time, old, updated *definition.Branch) {
	// Call plugins
	r.plugins.Executor().OnBranchSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.WriteOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.BranchKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.BranchKey).Put(r.client, *updated),
		)
	} else {
		// Save record to the "active" prefix
		saveCtx.WriteOp(r.schema.Active().ByKey(updated.BranchKey).Put(r.client, *updated))

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(now) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.WriteOp(r.schema.Deleted().ByKey(updated.BranchKey).Delete(r.client))
		}
	}
}
