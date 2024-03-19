package source

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source/schema"
	plugin2 "github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

const (
	MaxSourcesPerBranch        = 100
	MaxSourceVersionsPerSource = 1000
)

type Repository struct {
	client   etcd.KV
	schema   schema.Source
	plugins  *plugin2.Plugins
	branches *branch.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin2.Plugins
}

func NewRepository(d dependencies, branches *branch.Repository) *Repository {
	r := &Repository{
		client:   d.EtcdClient(),
		schema:   schema.ForSource(d.EtcdSerde()),
		plugins:  d.Plugins(),
		branches: branches,
	}

	// Delete/undelete source with branch
	r.plugins.Collection().OnBranchSave(func(ctx *plugin2.SaveContext, v *definition.Branch) {
		if v.DeletedAt != nil && v.DeletedAt.Time().Equal(ctx.Now()) {
			ctx.AddAtomicOp(r.softDeleteAllFrom(v.BranchKey, ctx.Now(), true))
		} else if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(ctx.Now()) {
			ctx.AddAtomicOp(r.undeleteAllFrom(v.BranchKey, ctx.Now(), true))
		}
	})

	return r
}

func (r *Repository) List(parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
	return r.list(r.schema.Active(), parentKey, opts...)
}

func (r *Repository) ListDeleted(parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
	return r.list(r.schema.Deleted(), parentKey, opts...)
}

func (r *Repository) list(pfx schema.SourceInState, parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
	return pfx.In(parentKey).GetAll(r.client, opts...)
}

func (r *Repository) ExistsOrErr(k key.SourceKey) op.WithResult[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
		})
}

func (r *Repository) Get(k key.SourceKey) op.WithResult[definition.Source] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
		})
}

func (r *Repository) GetDeleted(k key.SourceKey) op.WithResult[definition.Source] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted source", k.SourceID.String(), "branch")
		})
}

//nolint:dupl // similar code is in the SinkRepository
func (r *Repository) Create(input *definition.Source, now time.Time, versionDescription string) *op.AtomicOp[definition.Source] {
	k := input.SourceKey
	var entity definition.Source
	var actual, deleted *op.KeyValueT[definition.Source]

	atomicOp := op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.branches.ExistsOrErr(entity.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(entity.BranchKey, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Entity must not exist
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("source", k.SourceID.String(), "branch")
			}
			return nil
		}).
		// Init the entity
		BeforeWrite(func(ctx context.Context) {
			entity = *input
		}).
		// Set version/state from the deleted value, if any
		BeforeWrite(func(context.Context) {
			if deleted != nil {
				entity.Version = deleted.Value.Version
				entity.SoftDeletable = deleted.Value.SoftDeletable
				entity.Undelete(now)
			}
		}).
		// Increment version
		BeforeWrite(func(context.Context) {
			entity.IncrementVersion(entity, now, versionDescription)
		}).
		// Update the input entity after a successful operation
		OnResult(func(entity definition.Source) {
			*input = entity
		})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, []definition.Source{entity})
	})

	return atomicOp
}

//nolint:dupl // similar code is in SinkRepository
func (r *Repository) Update(k key.SourceKey, now time.Time, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	var entity definition.Source
	atomicOp := op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.checkMaxSourcesVersionsPerSource(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&entity)).
		// Update the entity
		BeforeWriteOrErr(func(context.Context) error {
			if updated, err := updateFn(entity); err == nil {
				updated.IncrementVersion(entity, now, versionDescription)
				entity = updated
				return nil
			} else {
				return err
			}
		})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, []definition.Source{entity})
	})

	return atomicOp
}

func (r *Repository) SoftDelete(k key.SourceKey, now time.Time) *op.AtomicOp[definition.Source] {
	var entity definition.Source
	return op.Atomic(r.client, &entity).
		AddFrom(r.
			softDeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					entity = r[0]
				}
			}))
}

func (r *Repository) Undelete(k key.SourceKey, now time.Time) *op.AtomicOp[definition.Source] {
	var entity definition.Source
	return op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.branches.ExistsOrErr(k.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		AddFrom(r.
			undeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					entity = r[0]
				}
			}))
}

// Versions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *Repository) Versions(k key.SourceKey) iterator.DefinitionT[definition.Source] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch entity version.
// The method can be used also for deleted objects.
func (r *Repository) Version(k key.SourceKey, version definition.VersionNumber) op.WithResult[definition.Source] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+version.String(), "branch")
		})
}

//nolint:dupl // similar code is in the SinkRepository
func (r *Repository) Rollback(k key.SourceKey, now time.Time, to definition.VersionNumber) *op.AtomicOp[definition.Source] {
	var entity definition.Source
	var latest, targetVersion *op.KeyValueT[definition.Source]

	atomicOp := op.Atomic(r.client, &entity).
		// Get latest version to calculate next version number
		ReadOp(r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latest)).
		// Get target version
		ReadOp(r.schema.Versions().Of(k).Version(to).GetKV(r.client).WithResultTo(&targetVersion)).
		// Return the most significant error
		BeforeWriteOrErr(func(context.Context) error {
			if latest == nil {
				return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
			} else if targetVersion == nil {
				return serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+to.String(), "branch")
			}
			return nil
		}).
		// Prepare the new value
		BeforeWrite(func(context.Context) {
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			entity = targetVersion.Value
			entity.Version = latest.Value.Version
			entity.IncrementVersion(entity, now, versionDescription)
		})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, []definition.Source{entity})
	})

	return atomicOp
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, deletedWithParent bool) *op.AtomicOp[[]definition.Source] {
	var all []definition.Source
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(entity definition.Source) {
			all = []definition.Source{entity}
		}))
	default:
		atomicOp.ReadOp(r.List(parentKey).WithAllTo(&all))
	}

	// Mark deleted
	atomicOp.BeforeWrite(func(ctx context.Context) {
		for i := range all {
			v := &all[i]

			// Mark deleted
			v.Delete(now, deletedWithParent)
		}
	})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, all)
	})

	return atomicOp
}

// undeleteAllFrom the parent key.
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, undeletedWithParent bool) *op.AtomicOp[[]definition.Source] {
	var all []definition.Source
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(entity definition.Source) {
			all = []definition.Source{entity}
		}))
	default:
		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&all))
	}

	// r.all.sink.softDeleteAllFrom(now, k, true)

	// Mark undeleted
	atomicOp.BeforeWrite(func(ctx context.Context) {
		for i := range all {
			v := &all[i]

			if v.DeletedWithParent != undeletedWithParent {
				continue
			}

			// Mark undeleted
			v.Undelete(now)

			// Create a new version record, if the entity has been undeleted manually
			if !undeletedWithParent {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, v.Version.Number)
				v.IncrementVersion(v, now, versionDescription)
			}
		}
	})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, all)
	})

	return atomicOp
}

//nolint:dupl // similar to SinkRepository.save
func (r *Repository) save(ctx context.Context, now time.Time, all []definition.Source) (op.Op, error) {
	saveCtx := plugin2.NewSaveContext(now)
	for _, v := range all {
		// Call plugins
		r.plugins.Executor().OnSourceSave(saveCtx, &v)

		if v.Deleted {
			// Move entity from the active prefix to the deleted prefix
			saveCtx.AddOp(
				// Delete entity from the active prefix
				r.schema.Active().ByKey(v.SourceKey).Delete(r.client),
				// Save entity to the deleted prefix
				r.schema.Deleted().ByKey(v.SourceKey).Put(r.client, v),
			)
		} else {
			saveCtx.AddOp(
				// Save record to the "active" prefix
				r.schema.Active().ByKey(v.SourceKey).Put(r.client, v),
				// Save record to the versions history
				r.schema.Versions().Of(v.SourceKey).Version(v.VersionNumber()).Put(r.client, v),
			)

			if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(now) {
				// Delete record from the "deleted" prefix, if needed
				saveCtx.AddOp(r.schema.Deleted().ByKey(v.SourceKey).Delete(r.client))
			}
		}
	}

	return saveCtx.Apply(ctx)
}

func (r *Repository) checkMaxSourcesPerBranch(k key.BranchKey, newCount int64) op.Op {
	return r.schema.
		Active().InBranch(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSourcesPerBranch {
				return serviceError.NewCountLimitReachedError("source", MaxSourcesPerBranch, "branch")
			}
			return nil
		})
}

func (r *Repository) checkMaxSourcesVersionsPerSource(k key.SourceKey, newCount int64) op.Op {
	return r.schema.
		Versions().Of(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSourceVersionsPerSource {
				return serviceError.NewCountLimitReachedError("version", MaxSourceVersionsPerSource, "source")
			}
			return nil
		})
}
