package source

import (
	"context"
	"fmt"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"
)

func (r *Repository) SoftDelete(k key.SourceKey, now time.Time) *op.AtomicOp[definition.Source] {
	var deleted definition.Source
	return op.Atomic(r.client, &deleted).
		AddFrom(r.
			softDeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					deleted = r[0]
				}
			}))
}

func (r *Repository) Undelete(k key.SourceKey, now time.Time) *op.AtomicOp[definition.Source] {
	var undeleted definition.Source
	return op.Atomic(r.client, &undeleted).
		// Check prerequisites
		ReadOp(r.branches.ExistsOrErr(k.BranchKey)).
		// Read the entity
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		// Mark undeleted
		AddFrom(r.
			undeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					undeleted = r[0]
				}
			}))
}

func (r *Repository) deleteSourcesOnBranchDelete() {
	r.plugins.Collection().OnBranchSave(func(ctx *plugin.SaveContext, old, entity *definition.Branch) {
		deleted := entity.DeletedAt != nil && entity.DeletedAt.Time().Equal(ctx.Now())
		if deleted {
			ctx.AddFrom(r.softDeleteAllFrom(entity.BranchKey, ctx.Now(), true))
		}
	})

}

func (r *Repository) undeleteSourcesOnBranchUndelete() {
	r.plugins.Collection().OnBranchSave(func(ctx *plugin.SaveContext, old, entity *definition.Branch) {
		undeleted := entity.UndeletedAt != nil && entity.UndeletedAt.Time().Equal(ctx.Now())
		if undeleted {
			ctx.AddFrom(r.undeleteAllFrom(entity.BranchKey, ctx.Now(), true))
		}
	})

}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, deletedWithParent bool) *op.AtomicOp[[]definition.Source] {
	var allOld, allDeleted []definition.Source
	atomicOp := op.Atomic(r.client, &allDeleted)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(entity definition.Source) { allOld = []definition.Source{entity} }))
	default:
		atomicOp.ReadOp(r.List(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		saveCtx := plugin.NewSaveContext(now)
		for _, old := range allOld {
			old := old

			// Mark deleted
			deleted := deepcopy.Copy(old).(definition.Source)
			deleted.Delete(now, deletedWithParent)

			// Save
			r.save(saveCtx, &old, &deleted)
			allDeleted = append(allDeleted, deleted)
		}
		return saveCtx.Do(ctx)
	})

	return atomicOp
}

// undeleteAllFrom the parent key.
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, undeletedWithParent bool) *op.AtomicOp[[]definition.Source] {
	var allOld, allCreated []definition.Source
	atomicOp := op.Atomic(r.client, &allCreated)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(entity definition.Source) { allOld = []definition.Source{entity} }))
	default:
		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		saveCtx := plugin.NewSaveContext(now)
		for _, old := range allOld {
			old := old

			if old.DeletedWithParent != undeletedWithParent {
				continue
			}

			// Mark undeleted
			created := deepcopy.Copy(old).(definition.Source)
			created.Undelete(now)

			// Create a new version record, if the entity has been undeleted manually
			if !undeletedWithParent {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, old.Version.Number)
				created.IncrementVersion(created, now, versionDescription)
			}

			// Save
			r.save(saveCtx, &old, &created)
			allCreated = append(allCreated, created)
		}
		return saveCtx.Do(ctx)
	})

	return atomicOp
}
