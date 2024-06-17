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
	var deleted *definition.Branch
	return op.Atomic(r.client, &created).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxBranchesPerProject(k.ProjectID, 1)
		}).
		// Entity must not exist
		Read(func(ctx context.Context) op.Op {
			return r.schema.Active().ByKey(k).GetOrErr(r.client).WithNotEmptyResultAsError(func() error {
				return serviceError.NewResourceAlreadyExistsError("branch", k.BranchID.String(), "project")
			})
		}).
		// Get deleted entity, if any, to undelete it
		Read(func(ctx context.Context) op.Op {
			return r.schema.Deleted().ByKey(k).GetOrNil(r.client).WithResultTo(&deleted)
		}).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create or undelete
			created = deepcopy.Copy(*input).(definition.Branch)
			if deleted != nil {
				created.Created = deleted.Created
				created.SoftDeletable = deleted.SoftDeletable
				created.Undelete(now, by)
			}

			// Save
			created.SetCreation(now, by)
			return r.save(ctx, now, by, nil, &created)
		}).
		// Update the input entity, it the operation is successful
		OnResult(func(entity definition.Branch) {
			*input = entity
		})
}
