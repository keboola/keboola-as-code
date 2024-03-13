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
	MaxSinksPerSource      = 100
	MaxSinkVersionsPerSink = 1000
)

type SinkRepository struct {
	client etcd.KV
	schema schema.Sink
	all    *Repository
}

func newSinkRepository(d dependencies, all *Repository) *SinkRepository {
	return &SinkRepository{
		client: d.EtcdClient(),
		schema: schema.ForSink(d.EtcdSerde()),
		all:    all,
	}
}

func (r *SinkRepository) List(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *SinkRepository) ListDeleted(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *SinkRepository) list(pfx schema.SinkInState, parentKey any) iterator.DefinitionT[definition.Sink] {
	return pfx.In(parentKey).GetAll(r.client)
}

func (r *SinkRepository) ExistsOrErr(k key.SinkKey) op.WithResult[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
		})
}

func (r *SinkRepository) Get(k key.SinkKey) op.WithResult[definition.Sink] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
		})
}

func (r *SinkRepository) GetDeleted(k key.SinkKey) op.WithResult[definition.Sink] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted sink", k.SinkID.String(), "source")
		})
}

//nolint:dupl // similar code is in the SourceRepository
func (r *SinkRepository) Create(rb rollback.Builder, now time.Time, versionDescription string, input *definition.Sink) *op.AtomicOp[definition.Sink] {
	k := input.SinkKey
	result := *input

	var actual, deleted *op.KeyValueT[definition.Sink]

	atomicOp := op.Atomic(r.client, &result).
		// Check prerequisites
		ReadOp(r.all.source.ExistsOrErr(result.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(result.SourceKey, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Entity must not exist
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("sink", k.SinkID.String(), "source")
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
		// Update the input entity after successful operation
		OnResult(func(result definition.Sink) {
			*input = result
		})

	// Save
	r.saveOne(rb, now, &result, atomicOp.Core())

	return atomicOp
}

//nolint:dupl // similar code is in the SourceRepository
func (r *SinkRepository) Update(rb rollback.Builder, now time.Time, k key.SinkKey, versionDescription string, updateFn func(definition.Sink) (definition.Sink, error)) *op.AtomicOp[definition.Sink] {
	var result definition.Sink
	atomicOp := op.Atomic(r.client, &result).
		// Check prerequisites
		ReadOp(r.checkMaxSinksVersionsPerSink(k, 1)).
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

func (r *SinkRepository) SoftDelete(rb rollback.Builder, now time.Time, k key.SinkKey) *op.AtomicOp[definition.Sink] {
	var result definition.Sink
	return op.Atomic(r.client, &result).
		AddFrom(r.
			softDeleteAllFrom(rb, now, k, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					result = r[0]
				}
			}))
}

func (r *SinkRepository) Undelete(rb rollback.Builder, now time.Time, k key.SinkKey) *op.AtomicOp[definition.Sink] {
	var result definition.Sink
	return op.Atomic(r.client, &result).
		// Check prerequisites
		ReadOp(r.all.source.ExistsOrErr(k.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		AddFrom(r.
			undeleteAllFrom(rb, now, k, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					result = r[0]
				}
			}))
}

// Versions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *SinkRepository) Versions(k key.SinkKey) iterator.DefinitionT[definition.Sink] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch entity version.
// The method can be used also for deleted objects.
func (r *SinkRepository) Version(k key.SinkKey, version definition.VersionNumber) op.WithResult[definition.Sink] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+version.String(), "source")
		})
}

//nolint:dupl // similar code is in the SourceRepository
func (r *SinkRepository) Rollback(rb rollback.Builder, now time.Time, k key.SinkKey, to definition.VersionNumber) *op.AtomicOp[definition.Sink] {
	var result definition.Sink
	var latestVersion, targetVersion *op.KeyValueT[definition.Sink]

	atomicOp := op.Atomic(r.client, &result).
		// Get latest version to calculate next version number
		ReadOp(r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latestVersion)).
		// Get target version
		ReadOp(r.schema.Versions().Of(k).Version(to).GetKV(r.client).WithResultTo(&targetVersion)).
		// Return the most significant error
		BeforeWriteOrErr(func(context.Context) error {
			if latestVersion == nil {
				return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
			} else if targetVersion == nil {
				return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+to.String(), "source")
			}
			return nil
		}).
		// Prepare the new value
		BeforeWrite(func(context.Context) {
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			result = targetVersion.Value
			result.Version = latestVersion.Value.Version
			result.IncrementVersion(result, now, versionDescription)
		})

	// Save
	r.saveOne(rb, now, &result, atomicOp.Core())

	return atomicOp
}

// softDeleteAllFrom the parent key.
func (r *SinkRepository) softDeleteAllFrom(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, deletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var all []definition.Sink
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(result definition.Sink) {
			all = []definition.Sink{result}
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

	return atomicOp
}

// undeleteAllFrom the parent key.
func (r *SinkRepository) undeleteAllFrom(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, undeletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var all []definition.Sink
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(result definition.Sink) {
			all = []definition.Sink{result}
		}))
	default:
		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&all))
	}

	// Iterate all
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

	return atomicOp
}

func (r *SinkRepository) saveOne(rb rollback.Builder, now time.Time, v *definition.Sink, atomicOp *op.AtomicOpCore) {
	var all []definition.Sink
	atomicOp.BeforeWrite(func(ctx context.Context) { all = []definition.Sink{*v} })
	r.saveAll(rb, now, v.SinkKey, &all, atomicOp)
}

//nolint:dupl // similar to SourceRepository.saveAll
func (r *SinkRepository) saveAll(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, all *[]definition.Sink, atomicOp *op.AtomicOpCore) {
	// Save
	atomicOp.Write(func(context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, v := range *all {
			if v.Deleted {
				// Move entity from the active prefix to the deleted prefix
				txn.Merge(
					// Delete entity from the active prefix
					r.schema.Active().ByKey(v.SinkKey).Delete(r.client),
					// Save entity to the deleted prefix
					r.schema.Deleted().ByKey(v.SinkKey).Put(r.client, v),
				)
			} else {
				txn.Merge(
					// Save record to the "active" prefix
					r.schema.Active().ByKey(v.SinkKey).Put(r.client, v),
					// Save record to the versions history
					r.schema.Versions().Of(v.SinkKey).Version(v.VersionNumber()).Put(r.client, v),
				)

				if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(now) {
					// Delete record from the "deleted" prefix, if needed
					txn.Merge(r.schema.Deleted().ByKey(v.SinkKey).Delete(r.client))
				}
			}
		}

		return txn
	})

	// Enrich atomic operation using hooks
	if r.all.hooks != nil {
		r.all.hooks.OnSinkSave(rb, now, parentKey, all, atomicOp)
	}
}

func (r *SinkRepository) checkMaxSinksPerSource(k key.SourceKey, newCount int64) op.Op {
	return r.schema.
		Active().InSource(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSinksPerSource {
				return serviceError.NewCountLimitReachedError("sink", MaxSinksPerSource, "source")
			}
			return nil
		})
}

func (r *SinkRepository) checkMaxSinksVersionsPerSink(k key.SinkKey, newCount int64) op.Op {
	return r.schema.
		Versions().Of(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSinkVersionsPerSink {
				return serviceError.NewCountLimitReachedError("version", MaxSinkVersionsPerSink, "sink")
			}
			return nil
		})
}
