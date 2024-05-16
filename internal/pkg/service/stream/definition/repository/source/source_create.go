package source

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

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
		// Entity must not exist
		Read(func(ctx context.Context) op.Op {
			return r.MustNotExists(k)
		}).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxSourcesPerBranch(k.BranchKey, 1)
		}).
		// Get deleted entity, if any, to undelete it
		Read(func(ctx context.Context) op.Op {
			return r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)
		}).
		// Create
		Write(func(ctx context.Context) op.Op {
			// Create or undelete
			created = deepcopy.Copy(*input).(definition.Source)
			if deleted != nil {
				created.Created = deleted.Value.Created
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now, by)
			}

			// Save
			created.SetCreation(now, by)
			created.IncrementVersion(created, now, by, versionDescription)
			return r.save(ctx, now, by, nil, &created)
		}).
		// Update the input entity, it the operation is successful
		OnResult(func(result definition.Source) {
			*input = result
		})
}
