package sink

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// PurgeAllFrom hard-deletes all Sinks of the given Source from etcd: each sink is removed from the
// active and deleted prefixes and its full version history is erased.
//
// Unlike SoftDelete, PurgeAllFrom does NOT trigger lifecycle plugins - it is meant to be called after
// a preceding SoftDelete (so the deactivation/delete hooks have already run). It exists to support
// cascade deletion of a source, where the sinks must be fully removed so the source can be recreated
// with the same key instead of being revived by Create.
func (r *Repository) PurgeAllFrom(sourceKey key.SourceKey) *op.AtomicOp[[]definition.Sink] {
	var purged []definition.Sink
	var active, deleted []definition.Sink
	return op.Atomic(r.client, &purged).
		Read(func(ctx context.Context) op.Op {
			return r.List(sourceKey).WithAllTo(&active)
		}).
		Read(func(ctx context.Context) op.Op {
			return r.ListDeleted(sourceKey).WithAllTo(&deleted)
		}).
		Write(func(ctx context.Context) op.Op {
			purged = append(purged, active...)
			purged = append(purged, deleted...)
			// Use prefix range-deletes (3 ops total) instead of per-sink deletes. A source may have
			// up to MaxSinksPerSource sinks, and 3 ops each would exceed etcd's per-transaction
			// operation limit. All sinks of a source share the same key prefix in every prefix
			// (active, deleted, version), so a single range-delete per prefix removes them all.
			return op.Txn(r.client).Then(
				r.schema.Active().InSource(sourceKey).DeleteAll(r.client),
				r.schema.Deleted().InSource(sourceKey).DeleteAll(r.client),
				r.schema.Versions().Add(sourceKey.String()).DeleteAll(r.client),
			)
		})
}
