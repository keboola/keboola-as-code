package source

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"
)

func (r *Repository) Update(k key.SourceKey, now time.Time, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	var old, updated definition.Source
	return op.Atomic(r.client, &updated).
		// Check prerequisites
		ReadOp(r.checkMaxSourcesVersionsPerSource(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Update
			updated = deepcopy.Copy(old).(definition.Source)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			updated.IncrementVersion(updated, now, versionDescription)
			return r.saveOne(ctx, now, &old, &updated)
		})
}
