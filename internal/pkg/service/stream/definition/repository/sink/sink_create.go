package sink

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
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
		// Check prerequisites
		ReadOp(r.sources.ExistsOrErr(k.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		// Entity must not exist
		ReadOp(r.schema.Active().ByKey(k).Get(r.client).WithNotEmptyResultAsError(func() error {
			return serviceError.NewResourceAlreadyExistsError("sink", k.SinkID.String(), "source")
		})).
		// Get deleted entity, if any, to undelete it
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create on undelete
			created = deepcopy.Copy(*input).(definition.Sink)
			if deleted != nil {
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now, by)
			}

			// Save
			created.IncrementVersion(created, now, by, versionDescription)
			return r.save(ctx, now, by, nil, &created)
		}).
		// Update the input entity after successful operation
		OnResult(func(entity definition.Sink) {
			*input = entity
		})

	return atomicOp
}
