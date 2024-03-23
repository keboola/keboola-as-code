package sink

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

func (r *Repository) SoftDelete(k key.SinkKey, now time.Time) *op.AtomicOp[definition.Sink] {
	var deleted definition.Sink
	return op.Atomic(r.client, &deleted).
		AddFrom(r.
			softDeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					deleted = r[0]
				}
			}))
}

func (r *Repository) Undelete(k key.SinkKey, now time.Time) *op.AtomicOp[definition.Sink] {
	var undeleted definition.Sink
	return op.Atomic(r.client, &undeleted).
		// Check prerequisites
		ReadOp(r.sources.ExistsOrErr(k.SourceKey)).
		// Read the entity
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		// Mark undeleted
		AddFrom(r.
			undeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					undeleted = r[0]
				}
			}))
}

func (r *Repository) deleteSinksOnSourceDelete() {
	r.plugins.Collection().OnSourceSave(func(ctx *plugin.SaveContext, old, entity *definition.Source) {
		deleted := entity.Deleted && entity.DeletedAt.Time().Equal(ctx.Now())
		if deleted {
			ctx.AddFrom(r.softDeleteAllFrom(entity.SourceKey, ctx.Now(), true))
		}
	})

}

func (r *Repository) undeleteSinksOnSourceUndelete() {
	r.plugins.Collection().OnSourceSave(func(ctx *plugin.SaveContext, old, entity *definition.Source) {
		undeleted := entity.UndeletedAt != nil && entity.UndeletedAt.Time().Equal(ctx.Now())
		if undeleted {
			ctx.AddFrom(r.undeleteAllFrom(entity.SourceKey, ctx.Now(), true))
		}
	})
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, deletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var allOld, allDeleted []definition.Sink
	atomicOp := op.Atomic(r.client, &allDeleted)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(entity definition.Sink) { allOld = []definition.Sink{entity} }))
	default:
		atomicOp.ReadOp(r.List(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		saveCtx := plugin.NewSaveContext(now)
		for _, old := range allOld {
			old := old

			// Mark deleted
			deleted := deepcopy.Copy(old).(definition.Sink)
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
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, undeletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var allOld, allCreated []definition.Sink
	atomicOp := op.Atomic(r.client, &allCreated)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(entity definition.Sink) { allOld = []definition.Sink{entity} }))
	default:
		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		allCreated = nil
		saveCtx := plugin.NewSaveContext(now)
		for _, old := range allOld {
			if old.DeletedWithParent != undeletedWithParent {
				continue
			}

			// Mark undeleted
			created := deepcopy.Copy(old).(definition.Sink)
			created.Undelete(now)

			// Create a new version record, if the entity has been undeleted manually
			if !undeletedWithParent {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, old.Version.Number)
				created.IncrementVersion(created, now, versionDescription)
			}

			// Save
			r.save(saveCtx, nil, &created)
			allCreated = append(allCreated, created)
		}
		return saveCtx.Do(ctx)
	})

	return atomicOp
}
