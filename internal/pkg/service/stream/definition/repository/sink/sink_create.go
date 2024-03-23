package sink

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"time"
)

//nolint:dupl // similar code is in the SourceRepository
func (r *Repository) Create(input *definition.Sink, now time.Time, versionDescription string) *op.AtomicOp[definition.Sink] {
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
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// Create on undelete
			created = deepcopy.Copy(*input).(definition.Sink)
			if deleted != nil {
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now)
			}

			// Save
			created.IncrementVersion(created, now, versionDescription)
			return r.saveOne(ctx, now, nil, &created)
		}).
		// Update the input entity after successful operation
		OnResult(func(entity definition.Sink) {
			*input = entity
		})

	return atomicOp
}
