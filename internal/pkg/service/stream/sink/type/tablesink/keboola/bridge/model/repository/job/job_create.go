package job

import (
	"context"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
)

// Create a new stream Job.
//
// - If the Job already exists, the ResourceAlreadyExistsError is returned.
func (r *Repository) Create(input *model.Job) *op.AtomicOp[model.Job] {
	k := input.JobKey
	var created model.Job
	return op.Atomic(r.client, &created).
		// Entity must not exist
		Read(func(ctx context.Context) op.Op {
			return r.MustNotExist(k)
		}).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create
			created = deepcopy.Copy(*input).(model.Job)
			return r.save(&created)
		}).
		// Update the input entity, it the operation is successful
		OnResult(func(result model.Job) {
			*input = result
		})
}
