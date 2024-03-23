package file

import (
	"context"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	etcd "go.etcd.io/etcd/client/v3"
	"time"
)

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *model.File) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Do(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *model.File) {
	// Call plugins
	r.plugins.Executor().OnFileSave(saveCtx, old, updated)

	allKey := r.schema.AllLevels().ByKey(updated.FileKey)
	inLevelKey := r.schema.InLevel(updated.State.Level()).ByKey(updated.FileKey)

	if updated.Deleted {
		// Delete entity from All and InLevel prefixes
		saveCtx.WriteOp(
			allKey.Delete(r.client),
			inLevelKey.Delete(r.client),
		)
	} else {
		if old == nil {
			// Entity should not exist
			saveCtx.WriteOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceAlreadyExistsError("file", updated.FileKey.String(), "sink"))
				}),
			)
		} else {
			// Entity should exist
			saveCtx.WriteOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("file", updated.FileKey.String(), "sink"))
				}),
			)
		}

		// Put entity to All and InLevel prefixes
		saveCtx.WriteOp(
			allKey.Put(r.client, *updated),
			inLevelKey.Put(r.client, *updated),
		)

		// Remove entity from the old InLevel prefix, if needed
		if old != nil && old.State.Level() != updated.State.Level() {
			saveCtx.WriteOp(
				r.schema.InLevel(old.State.Level()).ByKey(old.FileKey).Delete(r.client),
			)
		}
	}
}
