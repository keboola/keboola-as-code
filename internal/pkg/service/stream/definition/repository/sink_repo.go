package repository

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

const (
	MaxSinksPerSource      = 100
	MaxSinkVersionsPerSink = 1000
)

type SinkRepository struct {
	clock  clock.Clock
	client etcd.KV
	schema sinkSchema
	all    *Repository
}

func newSinkRepository(d dependencies, all *Repository) *SinkRepository {
	return &SinkRepository{
		clock:  d.Clock(),
		client: d.EtcdClient(),
		schema: newSinkSchema(d.EtcdSerde()),
		all:    all,
	}
}

func (r *SinkRepository) List(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *SinkRepository) ListDeleted(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *SinkRepository) list(pfx sinkSchemaInState, parentKey any) iterator.DefinitionT[definition.Sink] {
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
func (r *SinkRepository) Create(versionDescription string, input *definition.Sink) *op.AtomicOp[definition.Sink] {
	k := input.SinkKey
	result := *input

	var actual, deleted *op.KeyValueT[definition.Sink]

	return op.Atomic(r.client, &result).
		ReadOp(r.all.source.ExistsOrErr(result.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(result.SourceKey, 1)).
		// Get gets actual version to check if the object already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Object must not exists
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("sink", k.SinkID.String(), "source")
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
			result.IncrementVersion(result, r.clock.Now(), versionDescription)

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
		})
}

//nolint:dupl // similar code is in the SourceRepository
func (r *SinkRepository) Update(k key.SinkKey, updateVersion string, updateFn func(definition.Sink) definition.Sink) *op.AtomicOp[definition.Sink] {
	var result definition.Sink
	return op.Atomic(r.client, &result).
		ReadOp(r.checkMaxSinksVersionsPerSink(k, 1)).
		// Read and modify the object
		ReadOp(r.Get(k).WithResultTo(&result)).
		// Prepare the new value
		BeforeWrite(func(context.Context) {
			result = updateFn(result)
			result.IncrementVersion(result, r.clock.Now(), updateVersion)
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

func (r *SinkRepository) SoftDelete(k key.SinkKey) *op.AtomicOp[op.NoResult] {
	return r.softDelete(k, false)
}

func (r *SinkRepository) softDelete(k key.SinkKey, deletedWithParent bool) *op.AtomicOp[op.NoResult] {
	// Move object from the active to the deleted prefix
	var value definition.Sink
	return op.Atomic(r.client, &op.NoResult{}).
		// Move object from the active prefix to the deleted prefix
		ReadOp(r.Get(k).WithResultTo(&value)).
		Write(func(context.Context) op.Op { return r.softDeleteValue(value, deletedWithParent) })
}

// softDeleteAllFrom the parent key.
// All objects are marked with DeletedWithParent=true.
func (r *SinkRepository) softDeleteAllFrom(parentKey any) *op.AtomicOp[op.NoResult] {
	var writeOps []op.Op
	return op.Atomic(r.client, &op.NoResult{}).
		Read(func(context.Context) op.Op {
			writeOps = nil // reset after retry
			return r.List(parentKey).ForEach(func(v definition.Sink, _ *iterator.Header) error {
				writeOps = append(writeOps, r.softDeleteValue(v, true))
				return nil
			})
		}).
		Write(func(ctx context.Context) op.Op { return op.MergeToTxn(r.client, writeOps...) })
}

func (r *SinkRepository) softDeleteValue(v definition.Sink, deletedWithParent bool) *op.TxnOp[op.NoResult] {
	v.Delete(r.clock.Now(), deletedWithParent)
	return op.MergeToTxn(
		r.client,
		// Delete object from the active prefix
		r.schema.Active().ByKey(v.SinkKey).Delete(r.client),
		// Save object to the deleted prefix
		r.schema.Deleted().ByKey(v.SinkKey).Put(r.client, v),
	)
}

func (r *SinkRepository) Undelete(k key.SinkKey) *op.AtomicOp[definition.Sink] {
	// Move object from the deleted to the active prefix
	var result definition.Sink
	return op.Atomic(r.client, &result).
		ReadOp(r.all.source.ExistsOrErr(k.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		// Move object from the deleted prefix to the active prefix
		ReadOp(r.GetDeleted(k).WithResultTo(&result)).
		// Undelete
		Write(func(context.Context) op.Op { return r.undeleteValue(result) })
}

// undeleteAllFrom the parent key.
// Only object with DeletedWithParent=true are undeleted.
func (r *SinkRepository) undeleteAllFrom(parentKey any) *op.AtomicOp[op.NoResult] {
	var writeOps []op.Op
	return op.Atomic(r.client, &op.NoResult{}).
		Read(func(context.Context) op.Op {
			writeOps = nil // reset after retry
			return r.ListDeleted(parentKey).ForEach(func(v definition.Sink, _ *iterator.Header) error {
				if v.DeletedWithParent {
					writeOps = append(writeOps, r.undeleteValue(v))
				}
				return nil
			})
		}).
		Write(func(context.Context) op.Op { return op.MergeToTxn(r.client, writeOps...) })
}

func (r *SinkRepository) undeleteValue(v definition.Sink) *op.TxnOp[op.NoResult] {
	v.Undelete()
	return op.MergeToTxn(
		r.client,
		// Delete object from the deleted prefix
		r.schema.Deleted().ByKey(v.SinkKey).Delete(r.client),
		// Save object to the active prefix
		r.schema.Active().ByKey(v.SinkKey).Put(r.client, v),
	)
}

// Versions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *SinkRepository) Versions(k key.SinkKey) iterator.DefinitionT[definition.Sink] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch object version.
// The method can be used also for deleted objects.
func (r *SinkRepository) Version(k key.SinkKey, version definition.VersionNumber) op.WithResult[definition.Sink] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+version.String(), "source")
		})
}

//nolint:dupl // similar code is in the SourceRepository
func (r *SinkRepository) Rollback(k key.SinkKey, to definition.VersionNumber) *op.AtomicOp[definition.Sink] {
	var result definition.Sink
	var latestVersion, targetVersion *op.KeyValueT[definition.Sink]

	return op.Atomic(r.client, &result).
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
			result = targetVersion.Value
			result.Version = latestVersion.Value.Version
			result.IncrementVersion(result, r.clock.Now(), fmt.Sprintf("Rollback to version %d", targetVersion.Value.Version.Number))
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
