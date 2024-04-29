package branch

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

// Create a branch (that already exists in the Storage API) in the Stream Service database.
// - If there is a deleted Branch with the same key, the Undelete operation is performed.
// - If the Branch already exists, the ResourceAlreadyExistsError is returned.
// - If the MaxBranchesPerProject limit is exceeded, the CountLimitReachedError is returned.
func (r *Repository) Create(input *definition.Branch, now time.Time, by definition.By) *op.AtomicOp[definition.Branch] {
	k := input.BranchKey
	var created definition.Branch
	var deleted *op.KeyValueT[definition.Branch]
	return op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.checkMaxBranchesPerProject(k.ProjectID, 1)).
		// Entity must not exist
		ReadOp(r.schema.Active().ByKey(k).Get(r.client).WithNotEmptyResultAsError(func() error {
			return serviceError.NewResourceAlreadyExistsError("branch", k.BranchID.String(), "project")
		})).
		// Get deleted entity, if any, to undelete it
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create or undelete
			created = deepcopy.Copy(*input).(definition.Branch)
			if deleted != nil {
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now, by)
			}

			// Save
			return r.save(ctx, now, by, nil, &created)
		}).
		// Update the input entity, it the operation is successful
		OnResult(func(entity definition.Branch) {
			*input = entity
		})
}
