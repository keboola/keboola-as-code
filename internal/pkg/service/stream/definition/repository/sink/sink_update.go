package sink

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

func (r *Repository) Update(k key.SinkKey, now time.Time, versionDescription string, updateFn func(definition.Sink) (definition.Sink, error)) *op.AtomicOp[definition.Sink] {
	var old, updated definition.Sink
	return op.Atomic(r.client, &updated).
		// Check prerequisites
		ReadOp(r.checkMaxSinksVersionsPerSink(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Store old state
			disabled := old.Disabled
			deleted := old.Deleted

			// Update
			updated = deepcopy.Copy(old).(definition.Sink)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Disabled and Deleted fields cannot be modified by the Update operation
			if disabled != updated.Disabled {
				return nil, errors.Errorf(`"Disabled" field cannot be modified by the Update operation`)
			}
			if deleted != updated.Deleted {
				return nil, errors.Errorf(`"Deleted" field cannot be modified by the Update operation`)
			}

			// Save
			updated.IncrementVersion(updated, now, versionDescription)
			return r.saveOne(ctx, now, &old, &updated)
		})
}
