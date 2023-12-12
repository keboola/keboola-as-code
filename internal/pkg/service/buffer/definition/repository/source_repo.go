package repository

import (
	"fmt"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

const (
	MaxSourcesPerBranch        = 100
	MaxSourceVersionsPerSource = 1000
)

type SourceRepository struct {
	clock  clock.Clock
	client etcd.KV
	schema sourceSchema
	all    *Repository
}

func newSourceRepository(d dependencies, all *Repository) *SourceRepository {
	return &SourceRepository{
		clock:  d.Clock(),
		client: d.EtcdClient(),
		schema: newSourceSchema(d.EtcdSerde()),
		all:    all,
	}
}

func (r *SourceRepository) List(parentKey any) iterator.DefinitionT[definition.Source] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *SourceRepository) ListDeleted(parentKey any) iterator.DefinitionT[definition.Source] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *SourceRepository) list(pfx sourceSchemaInState, parentKey any) iterator.DefinitionT[definition.Source] {
	return pfx.In(parentKey).GetAll(r.client)
}

func (r *SourceRepository) ExistsOrErr(k key.SourceKey) op.ForType[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source", k.String(), "branch")
		})
}

func (r *SourceRepository) Get(k key.SourceKey) op.ForType[*op.KeyValueT[definition.Source]] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source", k.String(), "branch")
		})
}

func (r *SourceRepository) GetDeleted(k key.SourceKey) op.ForType[*op.KeyValueT[definition.Source]] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted source", k.String(), "branch")
		})
}

func (r *SourceRepository) Create(versionDescription string, input *definition.Source) *op.AtomicOp[definition.Source] {
	k := input.SourceKey
	result := *input

	var actual, deleted *op.KeyValueT[definition.Source]

	return op.Atomic(r.client, &result).
		ReadOp(r.all.branch.ExistsOrErr(result.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(result.BranchKey, 1)).
		// Get gets actual version to check if the object already exists
		ReadOp(r.schema.Active().ByKey(k).Get(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).Get(r.client).WithResultTo(&deleted)).
		// Object must not exists
		BeforeWriteOrErr(func() error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("source", k.String(), "branch")
			}
			return nil
		}).
		// Create or Undelete
		Write(func() op.Op {
			txn := op.NewTxnOp(r.client)

			// Was the object previously deleted?
			if deleted != nil {
				// Set version from the deleted value
				result.Version = deleted.Value.Version
				// Delete key from the "deleted" prefix, if any
				txn.Then(r.schema.Deleted().ByKey(k).Delete(r.client))
			}

			// Increment version and save
			result.IncrementVersion(result, r.clock.Now(), versionDescription)

			// Create the object
			txn.Then(r.schema.Active().ByKey(k).Put(r.client, result))

			// Save record to the versions history
			txn.Then(r.schema.Versions().Of(k).Version(result.VersionNumber()).Put(r.client, result))

			// Update the input entity after a successful operation
			txn.OnResult(func(r *op.TxnResult) {
				if r.Succeeded() {
					*input = result
				}
			})

			return txn
		}).
		AddFrom(r.all.sink.undeleteAllFrom(k))
}

func (r *SourceRepository) Update(k key.SourceKey, versionDescription string, updateFn func(definition.Source) definition.Source) *op.AtomicOp[definition.Source] {
	var result definition.Source
	var kv *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &result).
		ReadOp(r.checkMaxSourcesVersionsPerSource(k, 1)).
		// Read and modify the object
		ReadOp(r.Get(k).WithResultTo(&kv)).
		// Prepare the new value
		BeforeWrite(func() {
			result = kv.Value
			result = updateFn(result)
			result.IncrementVersion(result, r.clock.Now(), versionDescription)
		}).
		// Save the update object
		Write(func() op.Op {
			return r.schema.Active().ByKey(k).Put(r.client, result)
		}).
		// Save record to the versions history
		Write(func() op.Op {
			return r.schema.Versions().Of(k).Version(result.VersionNumber()).Put(r.client, result)
		})
}

func (r *SourceRepository) SoftDelete(k key.SourceKey) *op.AtomicOp[op.NoResult] {
	return r.softDelete(k, false)
}

func (r *SourceRepository) softDelete(k key.SourceKey, deletedWithParent bool) *op.AtomicOp[op.NoResult] {
	// Move object from the active to the deleted prefix
	var kv *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &op.NoResult{}).
		// Move object from the active prefix to the deleted prefix
		ReadOp(r.Get(k).WithResultTo(&kv)).
		Write(func() op.Op { return r.softDeleteValue(kv.Value, deletedWithParent) }).
		// Delete children
		AddFrom(r.all.sink.softDeleteAllFrom(k))
}

// softDeleteAllFrom the parent key.
// All objects are marked with DeletedWithParent=true.
func (r *SourceRepository) softDeleteAllFrom(parentKey any) *op.AtomicOp[op.NoResult] {
	var writeOps []op.Op
	return op.Atomic(r.client, &op.NoResult{}).
		Read(func() op.Op {
			writeOps = nil // reset after retry
			return r.List(parentKey).ForEachOp(func(v definition.Source, _ *iterator.Header) error {
				writeOps = append(writeOps, r.softDeleteValue(v, true))
				return nil
			})
		}).
		Write(func() op.Op { return op.MergeToTxn(r.client, writeOps...) }).
		// Delete children
		AddFrom(r.all.sink.softDeleteAllFrom(parentKey))
}

func (r *SourceRepository) softDeleteValue(v definition.Source, deletedWithParent bool) *op.TxnOp {
	v.Delete(r.clock.Now(), deletedWithParent)
	return op.MergeToTxn(
		r.client,
		// Delete object from the active prefix
		r.schema.Active().ByKey(v.SourceKey).Delete(r.client),
		// Save object to the deleted prefix
		r.schema.Deleted().ByKey(v.SourceKey).Put(r.client, v),
	)
}

func (r *SourceRepository) Undelete(k key.SourceKey) *op.AtomicOp[definition.Source] {
	// Move object from the deleted to the active prefix
	var result definition.Source
	var kv *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &result).
		ReadOp(r.all.branch.ExistsOrErr(k.BranchKey)).
		ReadOp(r.checkMaxSourcesPerBranch(k.BranchKey, 1)).
		// Move object from the deleted prefix to the active prefix
		ReadOp(r.GetDeleted(k).WithResultTo(&kv)).
		// Unwrap KV
		BeforeWrite(func() { result = kv.Value }).
		// Undelete
		Write(func() op.Op { return r.undeleteValue(kv.Value) }).
		// Undelete children
		AddFrom(r.all.sink.undeleteAllFrom(k))
}

// undeleteAllFrom the parent key.
// Only object with DeletedWithParent=true are undeleted.
func (r *SourceRepository) undeleteAllFrom(parentKey any) *op.AtomicOp[op.NoResult] {
	var writeOps []op.Op
	return op.Atomic(r.client, &op.NoResult{}).
		Read(func() op.Op {
			writeOps = nil // reset after retry
			return r.ListDeleted(parentKey).ForEachOp(func(v definition.Source, _ *iterator.Header) error {
				if v.DeletedWithParent {
					writeOps = append(writeOps, r.undeleteValue(v))
				}
				return nil
			})
		}).
		Write(func() op.Op { return op.MergeToTxn(r.client, writeOps...) }).
		// Undelete children
		AddFrom(r.all.sink.undeleteAllFrom(parentKey))
}

func (r *SourceRepository) undeleteValue(v definition.Source) *op.TxnOp {
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
func (r *SourceRepository) Version(k key.SourceKey, version definition.VersionNumber) op.ForType[*op.KeyValueT[definition.Source]] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source version", k.String()+"/"+version.String(), "branch")
		})
}

func (r *SourceRepository) Rollback(k key.SourceKey, to definition.VersionNumber) *op.AtomicOp[definition.Source] {
	var result definition.Source
	var latest, target *op.KeyValueT[definition.Source]

	return op.Atomic(r.client, &result).
		// Get latest version to calculate next version number
		ReadOp(r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latest)).
		// Get target version
		ReadOp(r.schema.Versions().Of(k).Version(to).Get(r.client).WithResultTo(&target)).
		// Return the most significant error
		BeforeWriteOrErr(func() error {
			if latest == nil {
				return serviceError.NewResourceNotFoundError("source", k.String(), "branch")
			} else if target == nil {
				return serviceError.NewResourceNotFoundError("source version", k.String()+"/"+to.String(), "branch")
			}
			return nil
		}).
		// Prepare the new value
		BeforeWrite(func() {
			result = target.Value
			result.Version = latest.Value.Version
			result.IncrementVersion(result, r.clock.Now(), fmt.Sprintf("Rollback to version %d", target.Value.Version.Number))
		}).
		// Save the object
		Write(func() op.Op {
			return r.schema.Active().ByKey(k).Put(r.client, result)
		}).
		// Save record to the versions history
		Write(func() op.Op {
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
