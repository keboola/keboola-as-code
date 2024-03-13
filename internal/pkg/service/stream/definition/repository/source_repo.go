package repository

import (
	"context"
	"fmt"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/schema"
)

const (
	MaxSourcesPerBranch        = 100
	MaxSourceVersionsPerSource = 1000
)

type SourceRepository struct {
	client etcd.KV
	schema schema.Source
	all    *Repository
}

func newSourceRepository(d dependencies, all *Repository) *SourceRepository {
	return &SourceRepository{
		client: d.EtcdClient(),
		schema: schema.ForSource(d.EtcdSerde()),
		all:    all,
	}
}

func (r *SourceRepository) List(parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
	return r.list(r.schema.Active(), parentKey, opts...)
}

func (r *SourceRepository) ListDeleted(parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
	return r.list(r.schema.Deleted(), parentKey, opts...)
}

func (r *SourceRepository) list(pfx schema.SourceInState, parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
	return pfx.In(parentKey).GetAll(r.client, opts...)
}

func (r *SourceRepository) ExistsOrErr(k key.SourceKey) op.WithResult[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
		})
}

func (r *SourceRepository) Get(k key.SourceKey) op.WithResult[definition.Source] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
		})
}

func (r *SourceRepository) GetDeleted(k key.SourceKey) op.WithResult[definition.Source] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted source", k.SourceID.String(), "branch")
		})
}

//nolint:dupl // similar code is in the SinkRepository
func (r *SourceRepository) Create(rb rollback.Builder, now time.Time, versionDescription string, input *definition.Source) *op.AtomicOp[definition.Source] {
	k := input.SourceKey
	result := *input

	var actual, deleted *op.KeyValueT[definition.Source]

	atomicOp := op.Atomic(r.client, &result).
		// Check prerequisites
		ReadOp(r.all.branch.ExistsOrErr(result.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(result.BranchKey, 1)).
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
		// Set version/state from the deleted value, if any
		BeforeWrite(func(context.Context) {
			if deleted != nil {
				result.Version = deleted.Value.Version
				result.SoftDeletable = deleted.Value.SoftDeletable
				result.Undelete(now)
			}
		}).
		// Increment version
		BeforeWrite(func(context.Context) {
			result.IncrementVersion(result, now, versionDescription)
		}).
		// Undelete nested sinks
		AddFrom(r.all.sink.undeleteAllFrom(rb, now, k, true)).
		// Update the input entity after a successful operation
		OnResult(func(result definition.Source) {
			*input = result
		})

	// Save
	r.saveOne(rb, now, &result, atomicOp.Core())

	return atomicOp
}

//nolint:dupl // similar code is in SinkRepository
func (r *SourceRepository) Update(rb rollback.Builder, now time.Time, k key.SourceKey, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	var result definition.Source
	atomicOp := op.Atomic(r.client, &result).
		// Check prerequisites
		ReadOp(r.checkMaxSourcesVersionsPerSource(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&result)).
		// Update the entity
		BeforeWriteOrErr(func(context.Context) error {
			if updated, err := updateFn(result); err == nil {
				updated.IncrementVersion(result, now, versionDescription)
				result = updated
				return nil
			} else {
				return err
			}
		})

	// Save
	r.saveOne(rb, now, &result, atomicOp.Core())

	return atomicOp
}

func (r *SourceRepository) SoftDelete(rb rollback.Builder, now time.Time, k key.SourceKey) *op.AtomicOp[definition.Source] {
	var result definition.Source
	return op.Atomic(r.client, &result).
		AddFrom(r.
			softDeleteAllFrom(rb, now, k, false).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					result = r[0]
				}
			}))
}

func (r *SourceRepository) Undelete(rb rollback.Builder, now time.Time, k key.SourceKey) *op.AtomicOp[definition.Source] {
	var result definition.Source
	return op.Atomic(r.client, &result).
		// Check prerequisites
		ReadOp(r.all.branch.ExistsOrErr(k.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		AddFrom(r.
			undeleteAllFrom(rb, now, k, false).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					result = r[0]
				}
			}))
}

// Versions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *SourceRepository) Versions(k key.SourceKey) iterator.DefinitionT[definition.Source] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch entity version.
// The method can be used also for deleted objects.
func (r *SourceRepository) Version(k key.SourceKey, version definition.VersionNumber) op.WithResult[definition.Source] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+version.String(), "branch")
		})
}

//nolint:dupl // similar code is in the SinkRepository
func (r *SourceRepository) Rollback(rb rollback.Builder, now time.Time, k key.SourceKey, to definition.VersionNumber) *op.AtomicOp[definition.Source] {
	var result definition.Source
	var latest, targetVersion *op.KeyValueT[definition.Source]

	atomicOp := op.Atomic(r.client, &result).
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
			result = targetVersion.Value
			result.Version = latest.Value.Version
			result.IncrementVersion(result, now, versionDescription)
		})

	// Save
	r.saveOne(rb, now, &result, atomicOp.Core())

	return atomicOp
}

// softDeleteAllFrom the parent key.
func (r *SourceRepository) softDeleteAllFrom(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, deletedWithParent bool) *op.AtomicOp[[]definition.Source] {
	var all []definition.Source
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(result definition.Source) {
			all = []definition.Source{result}
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
	r.saveAll(rb, now, parentKey, &all, atomicOp.Core())

	// Delete nested entities
	atomicOp.AddFrom(r.all.sink.softDeleteAllFrom(rb, now, parentKey, true))

	return atomicOp
}

// undeleteAllFrom the parent key.
func (r *SourceRepository) undeleteAllFrom(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, undeletedWithParent bool) *op.AtomicOp[[]definition.Source] {
	var all []definition.Source
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(result definition.Source) {
			all = []definition.Source{result}
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
	r.saveAll(rb, now, parentKey, &all, atomicOp.Core())

	// Undelete nested entities
	atomicOp.AddFrom(r.all.sink.undeleteAllFrom(rb, now, parentKey, true))

	return atomicOp
}

func (r *SourceRepository) saveOne(rb rollback.Builder, now time.Time, v *definition.Source, atomicOp *op.AtomicOpCore) {
	var all []definition.Source
	atomicOp.BeforeWrite(func(ctx context.Context) { all = []definition.Source{*v} })
	r.saveAll(rb, now, v.SourceKey, &all, atomicOp)
}

//nolint:dupl // similar to SinkRepository.saveAll
func (r *SourceRepository) saveAll(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, all *[]definition.Source, atomicOp *op.AtomicOpCore) {
	// Save
	atomicOp.Write(func(context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, v := range *all {
			if v.Deleted {
				// Move entity from the active prefix to the deleted prefix
				txn.Merge(
					// Delete entity from the active prefix
					r.schema.Active().ByKey(v.SourceKey).Delete(r.client),
					// Save entity to the deleted prefix
					r.schema.Deleted().ByKey(v.SourceKey).Put(r.client, v),
				)
			} else {
				txn.Merge(
					// Save record to the "active" prefix
					r.schema.Active().ByKey(v.SourceKey).Put(r.client, v),
					// Save record to the versions history
					r.schema.Versions().Of(v.SourceKey).Version(v.VersionNumber()).Put(r.client, v),
				)

				if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(now) {
					// Delete record from the "deleted" prefix, if needed
					txn.Merge(r.schema.Deleted().ByKey(v.SourceKey).Delete(r.client))
				}
			}
		}

		return txn
	})

	// Enrich atomic operation using hooks
	if r.all.hooks != nil {
		r.all.hooks.OnSourceSave(rb, now, parentKey, all, atomicOp)
	}
}

func (r *SourceRepository) checkMaxSourcesPerBranch(k key.BranchKey, newCount int64) op.Op {
	return r.schema.
		Active().InBranch(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSourcesPerBranch {
				return serviceError.NewCountLimitReachedError("source", MaxSourcesPerBranch, "branch")
			}
			return nil
		})
}

func (r *SourceRepository) checkMaxSourcesVersionsPerSource(k key.SourceKey, newCount int64) op.Op {
	return r.schema.
		Versions().Of(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSourceVersionsPerSource {
				return serviceError.NewCountLimitReachedError("version", MaxSourceVersionsPerSource, "source")
			}
			return nil
		})
}
