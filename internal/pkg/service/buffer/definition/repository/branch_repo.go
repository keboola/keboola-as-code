package repository

import (
	"context"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

const (
	MaxBranchesPerProject = 100
)

type BranchRepository struct {
	clock  clock.Clock
	client etcd.KV
	schema branchSchema
	all    *Repository
}

func newBranchRepository(d dependencies, all *Repository) *BranchRepository {
	return &BranchRepository{
		clock:  d.Clock(),
		client: d.EtcdClient(),
		schema: newBranchSchema(d.EtcdSerde()),
		all:    all,
	}
}

func (r *BranchRepository) List(parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *BranchRepository) ListDeleted(parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *BranchRepository) list(pfx branchSchemaInState, parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return pfx.InProject(parentKey).GetAll(r.client)
}

func (r *BranchRepository) ExistsOrErr(k key.BranchKey) op.WithResult[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("branch", k.String(), "project")
		})
}

func (r *BranchRepository) Get(k key.BranchKey) op.WithResult[definition.Branch] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("branch", k.String(), "project")
		})
}

func (r *BranchRepository) GetDeleted(k key.BranchKey) op.WithResult[definition.Branch] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted branch", k.String(), "project")
		})
}

func (r *BranchRepository) Create(input *definition.Branch) *op.AtomicOp[definition.Branch] {
	k := input.BranchKey
	result := *input

	var actual, deleted *op.KeyValueT[definition.Branch]

	return op.Atomic(r.client, &result).
		ReadOp(r.checkMaxBranchesPerProject(result.ProjectID, 1)).
		// Get gets actual version to check if the object already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Object must not exists
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("branch", k.String(), "project")
			}
			return nil
		}).
		// Create or Undelete
		Write(func(context.Context) op.Op {
			txn := op.Txn(r.client)

			// Was the object previously deleted?
			if deleted != nil {
				// Delete key from the "deleted" prefix, if any
				txn.Then(r.schema.Deleted().ByKey(k).Delete(r.client))
			}

			// Create the object
			txn.Then(r.schema.Active().ByKey(k).Put(r.client, result))

			// Update the input entity after a successful operation
			txn.OnResult(func(r *op.TxnResult[op.NoResult]) {
				if r.Succeeded() {
					*input = result
				}
			})

			return txn
		}).
		AddFrom(r.all.source.undeleteAllFrom(k))
}

func (r *BranchRepository) SoftDelete(k key.BranchKey) *op.AtomicOp[op.NoResult] {
	// Move object from the active to the deleted prefix
	var value definition.Branch
	return op.Atomic(r.client, &op.NoResult{}).
		// Move object from the active prefix to the deleted prefix
		ReadOp(r.Get(k).WithResultTo(&value)).
		Write(func(context.Context) op.Op { return r.softDeleteValue(value) }).
		// Delete children
		AddFrom(r.all.source.softDeleteAllFrom(k))
}

func (r *BranchRepository) softDeleteValue(v definition.Branch) *op.TxnOp[op.NoResult] {
	v.Delete(r.clock.Now(), false)
	return op.MergeToTxn(
		r.client,
		// Delete object from the active prefix
		r.schema.Active().ByKey(v.BranchKey).Delete(r.client),
		// Save object to the deleted prefix
		r.schema.Deleted().ByKey(v.BranchKey).Put(r.client, v),
	)
}

func (r *BranchRepository) Undelete(k key.BranchKey) *op.AtomicOp[definition.Branch] {
	// Move object from the deleted to the active prefix
	var result definition.Branch
	return op.Atomic(r.client, &result).
		ReadOp(r.checkMaxBranchesPerProject(k.ProjectID, 1)).
		// Move object from the deleted prefix to the active prefix
		ReadOp(r.GetDeleted(k).WithResultTo(&result)).
		// Undelete
		Write(func(context.Context) op.Op { return r.undeleteValue(result) }).
		// Undelete children
		AddFrom(r.all.source.undeleteAllFrom(k))
}

func (r *BranchRepository) undeleteValue(v definition.Branch) *op.TxnOp[op.NoResult] {
	v.Undelete()
	return op.MergeToTxn(
		r.client,
		// Delete object from the deleted prefix
		r.schema.Deleted().ByKey(v.BranchKey).Delete(r.client),
		// Save object to the active prefix
		r.schema.Active().ByKey(v.BranchKey).Put(r.client, v),
	)
}

func (r *BranchRepository) checkMaxBranchesPerProject(k keboola.ProjectID, newCount int64) op.Op {
	return r.schema.
		Active().InProject(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxBranchesPerProject {
				return serviceError.NewCountLimitReachedError("branch", MaxBranchesPerProject, "project")
			}
			return nil
		})
}
