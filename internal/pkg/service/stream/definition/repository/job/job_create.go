package job

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

// Create a new stream Job.
//
// - If the Job already exists, the ResourceAlreadyExistsError is returned.
func (r *Repository) Create(now time.Time, input *definition.Job) *op.AtomicOp[definition.Job] {
	k := input.JobKey
	var created definition.Job
	return op.Atomic(r.client, &created).
		// Entity must not exist
		Read(func(ctx context.Context) op.Op {
			return r.MustNotExist(k)
		}).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create
			created = deepcopy.Copy(*input).(definition.Job)
			saveTxn := op.TxnWithResult(r.client, &created)
			if created.IsDeleted() {
				// Move entity from the active prefix to the deleted prefix
				saveTxn.Then(
					// Delete entity from the active prefix
					r.schema.Active().ByKey(created.JobKey).Delete(r.client),
					// Save entity to the deleted prefix
					r.schema.Deleted().ByKey(created.JobKey).Put(r.client, created),
				)
			} else {
				saveTxn.Then(
					// Save record to the "active" prefix
					r.schema.Active().ByKey(created.JobKey).Put(r.client, created),
				)

				if created.IsUndeletedAt(now) {
					// Delete record from the "deleted" prefix, if needed
					saveTxn.Then(r.schema.Deleted().ByKey(created.JobKey).Delete(r.client))
				}
			}
			return saveTxn
		}).
		// Update the input entity, it the operation is successful
		OnResult(func(result definition.Job) {
			*input = result
		})
}
