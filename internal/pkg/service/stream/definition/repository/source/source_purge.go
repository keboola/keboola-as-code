package source

import (
	"context"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Purge hard-deletes a Source from etcd: it removes the entity from the active and deleted prefixes
// and erases its full version history.
//
// Unlike SoftDelete, Purge does NOT trigger lifecycle plugins - it is meant to be called after a
// preceding SoftDelete (so the deactivation/delete hooks have already run). It exists to support
// cascade deletion, where the source must be fully removed so it can be recreated with the same key
// instead of being revived by Create.
func (r *Repository) Purge(k key.SourceKey) *op.AtomicOp[definition.Source] {
	var purged definition.Source
	var active, deleted *definition.Source
	return op.Atomic(r.client, &purged).
		Read(func(ctx context.Context) op.Op {
			return r.schema.Active().ByKey(k).GetOrNil(r.client).WithResultTo(&active)
		}).
		Read(func(ctx context.Context) op.Op {
			return r.schema.Deleted().ByKey(k).GetOrNil(r.client).WithResultTo(&deleted)
		}).
		Write(func(ctx context.Context) op.Op {
			// The source must exist (in either prefix); otherwise behave like the other repo ops.
			if active == nil && deleted == nil {
				return op.ErrorOp(serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch"))
			}
			if deleted != nil {
				purged = *deleted
			}
			if active != nil {
				purged = *active
			}
			return op.Txn(r.client).Then(
				r.schema.Active().ByKey(k).Delete(r.client),
				r.schema.Deleted().ByKey(k).Delete(r.client),
				r.schema.Versions().Of(k).DeleteAll(r.client),
			)
		})
}
