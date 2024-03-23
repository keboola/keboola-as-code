package file

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

// update reads the file, applies updateFn and save modified value.
func (r *Repository) update(k model.FileKey, now time.Time, updateFn func(model.File) (model.File, error)) *op.AtomicOp[model.File] {
	var old, updated model.File
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Update
			updated = deepcopy.Copy(old).(model.File)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			return r.saveOne(ctx, now, &old, &updated)
		})
}
