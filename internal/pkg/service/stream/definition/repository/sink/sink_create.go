package sink

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

// Create a new stream Sink.
// - If there is a deleted Sink with the same key, the Undelete operation is performed.
// - If the Sink already exists, the ResourceAlreadyExistsError is returned.
// - If the MaxSinksPerSource limit is exceeded, the CountLimitReachedError is returned.
// - If the MaxSinkVersionsPerSink limit is exceeded, the CountLimitReachedError is returned.
func (r *Repository) Create(input *definition.Sink, now time.Time, by definition.By, versionDescription string) *op.AtomicOp[definition.Sink] {
	k := input.SinkKey
	var created definition.Sink
	var deleted *op.KeyValueT[definition.Sink]
	atomicOp := op.Atomic(r.client, &created).
		// Entity must not exist
		Read(func(ctx context.Context) op.Op {
			return r.MustNotExists(k)
		}).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxSinksPerSource(k.SourceKey, 1)
		}).
		// Get deleted entity, if any, to undelete it
		Read(func(ctx context.Context) op.Op {
			return r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)
		}).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create on undelete
			created = deepcopy.Copy(*input).(definition.Sink)
			if deleted != nil {
				created.Created = deleted.Value.Created
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now, by)
			}

			// Save
			created.SetCreation(now, by)
			created.IncrementVersion(created, now, by, versionDescription)
			return r.save(ctx, now, by, nil, &created)
		}).
		// Update the input entity, it the operation is successful
		OnResult(func(entity definition.Sink) {
			*input = entity
		})

	return atomicOp
}
