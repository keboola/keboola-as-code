package source

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

const (
	MaxSourcesPerBranch        = 100
	MaxSourceVersionsPerSource = 1000
)

type Repository struct {
	client   etcd.KV
	schema   schema.Source
	plugins  *plugin.Plugins
	branches *branch.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies, branches *branch.Repository) *Repository {
	r := &Repository{
		client:   d.EtcdClient(),
		schema:   schema.ForSource(d.EtcdSerde()),
		plugins:  d.Plugins(),
		branches: branches,
	}

	// Delete/undelete source with branch
	r.plugins.Collection().OnBranchSave(func(ctx *plugin.SaveContext, old, updated *definition.Branch) {
		deleted := updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(ctx.Now())
		undeleted := updated.DeletedAt != nil && updated.DeletedAt.Time().Equal(ctx.Now())
		if undeleted {
			ctx.AddAtomicOp(r.softDeleteAllFrom(updated.BranchKey, ctx.Now(), true))
		} else if deleted {
			ctx.AddAtomicOp(r.undeleteAllFrom(updated.BranchKey, ctx.Now(), true))
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

func (r *Repository) Create(input *definition.Source, now time.Time, versionDescription string) *op.AtomicOp[definition.Source] {
	k := input.SourceKey
	var created definition.Source
	var actual, deleted *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.branches.ExistsOrErr(k.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// Entity must not exist
			if actual != nil {
				return nil, serviceError.NewResourceAlreadyExistsError("source", k.SourceID.String(), "branch")
			}

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

func (r *Repository) Update(k key.SourceKey, now time.Time, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	var old, updated definition.Source
	return op.Atomic(r.client, &updated).
		// Check prerequisites
		ReadOp(r.checkMaxSourcesVersionsPerSource(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Update
			updated = deepcopy.Copy(old).(definition.Source)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			updated.IncrementVersion(updated, now, versionDescription)
			return r.saveOne(ctx, now, &old, &updated)
		})
}

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
	var updated definition.Source
	var latest, targetVersion *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &updated).
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
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			old := targetVersion.Value
			updated = deepcopy.Copy(old).(definition.Source)
			updated.Version = latest.Value.Version
			updated.IncrementVersion(updated, now, versionDescription)
			return r.saveOne(ctx, now, &old, &updated)
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
		return saveCtx.Apply(ctx)
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
		return saveCtx.Apply(ctx)
	})

	return atomicOp
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Source) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Apply(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *definition.Source) {
	// Call plugins
	r.plugins.Executor().OnSourceSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.AddOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.SourceKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.SourceKey).Put(r.client, *updated),
		)
	} else {
		saveCtx.AddOp(
			// Save record to the "active" prefix
			r.schema.Active().ByKey(updated.SourceKey).Put(r.client, *updated),
			// Save record to the versions history
			r.schema.Versions().Of(updated.SourceKey).Version(updated.VersionNumber()).Put(r.client, *updated),
		)

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(saveCtx.Now()) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.AddOp(r.schema.Deleted().ByKey(updated.SourceKey).Delete(r.client))
		}
	}
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
