package repository

import (
	"context"
	"fmt"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
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
func (r *SourceRepository) Create(now time.Time, versionDescription string, input *definition.Source) *op.AtomicOp[definition.Source] {
	k := input.SourceKey
	result := *input

	var actual, deleted *op.KeyValueT[definition.Source]

	return op.Atomic(r.client, &result).
		ReadOp(r.all.branch.ExistsOrErr(result.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(result.BranchKey, 1)).
		// Get gets actual version to check if the object already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Object must not exists
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("source", k.SourceID.String(), "branch")
			}
			return nil
		}).
		// Create or Undelete
		Write(func(context.Context) op.Op {
			txn := op.Txn(r.client)

			// Was the object previously deleted?
			if deleted != nil {
				// Set version from the deleted value
				result.Version = deleted.Value.Version
				// Delete key from the "deleted" prefix, if any
				txn.Then(r.schema.Deleted().ByKey(k).Delete(r.client))
			}

			// Increment version and save
			result.IncrementVersion(result, now, versionDescription)

			// Create the object
			txn.Then(r.schema.Active().ByKey(k).Put(r.client, result))

			// Save record to the versions history
			txn.Then(r.schema.Versions().Of(k).Version(result.VersionNumber()).Put(r.client, result))

			// Update the input entity after a successful operation
			txn.OnResult(func(r *op.TxnResult[op.NoResult]) {
				if r.Succeeded() {
					*input = result
				}
			})

			return txn
		}).
		AddFrom(r.all.sink.undeleteAllFrom(now, k))
}

//nolint:dupl // similar code is in SinkRepository
func (r *SourceRepository) Update(now time.Time, k key.SourceKey, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	var result definition.Source
	return op.Atomic(r.client, &result).
		ReadOp(r.checkMaxSourcesVersionsPerSource(k, 1)).
		// Read and modify the object
		ReadOp(r.Get(k).WithResultTo(&result)).
		// Prepare the new value
		BeforeWriteOrErr(func(context.Context) (err error) {
			if result, err = updateFn(result); err != nil {
				return err
			}

			result.IncrementVersion(result, now, versionDescription)
			return nil
		}).
		// Save the update object
		Write(func(context.Context) op.Op {
			return r.schema.Active().ByKey(k).Put(r.client, result)
		}).
		// Save record to the versions history
		Write(func(context.Context) op.Op {
			return r.schema.Versions().Of(k).Version(result.VersionNumber()).Put(r.client, result)
		})
}

func (r *SourceRepository) SoftDelete(now time.Time, k key.SourceKey) *op.AtomicOp[op.NoResult] {
	return r.softDelete(now, k, false)
}

func (r *SourceRepository) softDelete(now time.Time, k key.SourceKey, deletedWithParent bool) *op.AtomicOp[op.NoResult] {
	// Move object from the active to the deleted prefix
	var value definition.Source
	return op.Atomic(r.client, &op.NoResult{}).
		// Move object from the active prefix to the deleted prefix
		ReadOp(r.Get(k).WithResultTo(&value)).
		Write(func(context.Context) op.Op { return r.softDeleteValue(now, value, deletedWithParent) }).
		// Delete children
		AddFrom(r.all.sink.softDeleteAllFrom(now, k))
}

// softDeleteAllFrom the parent key.
// All objects are marked with DeletedWithParent=true.
func (r *SourceRepository) softDeleteAllFrom(now time.Time, parentKey any) *op.AtomicOp[op.NoResult] {
	var writeOps []op.Op
	return op.Atomic(r.client, &op.NoResult{}).
		Read(func(context.Context) op.Op {
			writeOps = nil // reset after retry
			return r.List(parentKey).ForEach(func(v definition.Source, _ *iterator.Header) error {
				writeOps = append(writeOps, r.softDeleteValue(now, v, true))
				return nil
			})
		}).
		Write(func(ctx context.Context) op.Op { return op.MergeToTxn(r.client, writeOps...) }).
		// Delete children
		AddFrom(r.all.sink.softDeleteAllFrom(now, parentKey))
}

func (r *SourceRepository) softDeleteValue(now time.Time, v definition.Source, deletedWithParent bool) *op.TxnOp[op.NoResult] {
	v.Delete(now, deletedWithParent)
	return op.MergeToTxn(
		r.client,
		// Delete object from the active prefix
		r.schema.Active().ByKey(v.SourceKey).Delete(r.client),
		// Save object to the deleted prefix
		r.schema.Deleted().ByKey(v.SourceKey).Put(r.client, v),
	)
}

func (r *SourceRepository) Undelete(now time.Time, k key.SourceKey) *op.AtomicOp[definition.Source] {
	// Move object from the deleted to the active prefix
	var result definition.Source
	return op.Atomic(r.client, &result).
		ReadOp(r.all.branch.ExistsOrErr(k.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		// Move object from the deleted prefix to the active prefix
		ReadOp(r.GetDeleted(k).WithResultTo(&result)).
		// Undelete
		Write(func(context.Context) op.Op { return r.undeleteValue(result) }).
		// Undelete children
		AddFrom(r.all.sink.undeleteAllFrom(now, k))
}

// undeleteAllFrom the parent key.
// Only object with DeletedWithParent=true are undeleted.
func (r *SourceRepository) undeleteAllFrom(now time.Time, parentKey any) *op.AtomicOp[op.NoResult] {
	var writeOps []op.Op
	return op.Atomic(r.client, &op.NoResult{}).
		Read(func(context.Context) op.Op {
			writeOps = nil // reset after retry
			return r.ListDeleted(parentKey).ForEach(func(v definition.Source, _ *iterator.Header) error {
				if v.DeletedWithParent {
					writeOps = append(writeOps, r.undeleteValue(v))
				}
				return nil
			})
		}).
		Write(func(context.Context) op.Op { return op.MergeToTxn(r.client, writeOps...) }).
		// Undelete children
		AddFrom(r.all.sink.undeleteAllFrom(now, parentKey))
}

func (r *SourceRepository) undeleteValue(v definition.Source) *op.TxnOp[op.NoResult] {
	v.Undelete()
	return op.MergeToTxn(
		r.client,
		// Delete object from the deleted prefix
		r.schema.Deleted().ByKey(v.SourceKey).Delete(r.client),
		// Save object to the active prefix
		r.schema.Active().ByKey(v.SourceKey).Put(r.client, v),
	)
}

// Versions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *SourceRepository) Versions(k key.SourceKey) iterator.DefinitionT[definition.Source] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch object version.
// The method can be used also for deleted objects.
func (r *SourceRepository) Version(k key.SourceKey, version definition.VersionNumber) op.WithResult[definition.Source] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+version.String(), "branch")
		})
}

//nolint:dupl // similar code is in the SinkRepository
func (r *SourceRepository) Rollback(now time.Time, k key.SourceKey, to definition.VersionNumber) *op.AtomicOp[definition.Source] {
	var result definition.Source
	var latest, target *op.KeyValueT[definition.Source]

	return op.Atomic(r.client, &result).
		// Get latest version to calculate next version number
		ReadOp(r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latest)).
		// Get target version
		ReadOp(r.schema.Versions().Of(k).Version(to).GetKV(r.client).WithResultTo(&target)).
		// Return the most significant error
		BeforeWriteOrErr(func(context.Context) error {
			if latest == nil {
				return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
			} else if target == nil {
				return serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+to.String(), "branch")
			}
			return nil
		}).
		// Prepare the new value
		BeforeWrite(func(context.Context) {
			result = target.Value
			result.Version = latest.Value.Version
			result.IncrementVersion(result, now, fmt.Sprintf("Rollback to version %d", target.Value.Version.Number))
		}).
		// Save the object
		Write(func(context.Context) op.Op {
			return r.schema.Active().ByKey(k).Put(r.client, result)
		}).
		// Save record to the versions history
		Write(func(context.Context) op.Op {
			return r.schema.Versions().Of(k).Version(result.VersionNumber()).Put(r.client, result)
		})
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
