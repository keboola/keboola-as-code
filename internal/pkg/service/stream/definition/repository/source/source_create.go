package source

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

// Create a new stream Source.
// - If there is a deleted Source with the same key, the Undelete operation is performed.
// - If the Source already exists, the ResourceAlreadyExistsError is returned.
// - If the MaxSourcesPerBranch limit is exceeded, the CountLimitReachedError is returned.
// - If the MaxSourceVersionsPerSource limit is exceeded, the CountLimitReachedError is returned.
func (r *Repository) Create(input *definition.Source, now time.Time, by definition.By, versionDescription string) *op.AtomicOp[definition.Source] {
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
		// Get deleted entity, if any, to undelete it
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create or undelete
			created = deepcopy.Copy(*input).(definition.Source)
			if deleted != nil {
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now, by)
			}

			// Save
			created.IncrementVersion(created, now, by, versionDescription)
			return r.save(ctx, now, by, nil, &created)
		}).
		// Update the input entity after a successful operation
		OnResult(func(result definition.Source) {
			*input = result
		})
}
