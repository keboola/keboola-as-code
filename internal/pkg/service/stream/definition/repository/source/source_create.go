package source

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"time"
)

func (r *Repository) Create(input *definition.Source, now time.Time, versionDescription string) *op.AtomicOp[definition.Source] {
	k := input.SourceKey
	var created definition.Source
	var deleted *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.branches.ExistsOrErr(k.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		// Entity must not exist
		ReadOp(r.schema.Active().ByKey(k).Get(r.client).WithNotEmptyResultAsError(func() error {
			return serviceError.NewResourceAlreadyExistsError("source", k.SourceID.String(), "branch")
		})).
		// Get deleted entity, if any, to undelete it.
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// Create or undelete
			created = deepcopy.Copy(*input).(definition.Source)
			if deleted != nil {
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now)
			}

			// Save
			created.IncrementVersion(created, now, versionDescription)
			return r.saveOne(ctx, now, nil, &created)
		}).
		// Update the input entity after a successful operation
		OnResult(func(result definition.Source) {
			*input = result
		})
}
