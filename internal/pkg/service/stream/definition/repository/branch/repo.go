package branch

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
	plugin2 "github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

const (
	MaxBranchesPerProject = 100
)

type Repository struct {
	client  etcd.KV
	plugins *plugin2.Plugins
	schema  schema.Branch
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin2.Plugins
}

func NewRepository(d dependencies) *Repository {
	return &Repository{
		client:  d.EtcdClient(),
		schema:  schema.ForBranch(d.EtcdSerde()),
		plugins: d.Plugins(),
	}
}

func (r *Repository) List(parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *Repository) ListDeleted(parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *Repository) list(pfx schema.BranchInState, parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return pfx.InProject(parentKey).GetAll(r.client)
}

func (r *Repository) ExistsOrErr(k key.BranchKey) op.WithResult[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("branch", k.BranchID.String(), "project")
		})
}

func (r *Repository) GetDefault(k keboola.ProjectID) *op.TxnOp[definition.Branch] {
	found := false
	var entity definition.Branch
	return op.
		TxnWithResult(r.client, &entity).
		Then(r.List(k).ForEach(func(branch definition.Branch, _ *iterator.Header) error {
			if branch.IsDefault {
				found = true
				entity = branch
			}
			return nil
		})).
		OnSucceeded(func(r *op.TxnResult[definition.Branch]) {
			if !found {
				r.AddErr(serviceError.NewResourceNotFoundError("branch", "default", "project"))
			}
		})
}

func (r *Repository) Get(k key.BranchKey) op.WithResult[definition.Branch] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("branch", k.BranchID.String(), "project")
		})
}

func (r *Repository) GetDeleted(k key.BranchKey) op.WithResult[definition.Branch] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted branch", k.BranchID.String(), "project")
		})
}

func (r *Repository) Create(input *definition.Branch, now time.Time) *op.AtomicOp[definition.Branch] {
	k := input.BranchKey
	var entity definition.Branch
	var actual, deleted *op.KeyValueT[definition.Branch]

	return op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.checkMaxBranchesPerProject(entity.ProjectID, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Entity must not exist
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("branch", k.BranchID.String(), "project")
			}
			return nil
		}).
		// Init the entity
		BeforeWrite(func(context.Context) {
			entity = *input
		}).
		// Set state from the deleted value, if any
		BeforeWrite(func(context.Context) {
			if deleted != nil {
				entity.SoftDeletable = deleted.Value.SoftDeletable
				entity.Undelete(now)
			}
		}).
		// Save
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			return r.save(ctx, now, []definition.Branch{entity})
		}).
		// Update the input entity after a successful operation
		OnResult(func(entity definition.Branch) {
			*input = entity
		})
}

func (r *Repository) SoftDelete(k key.BranchKey, now time.Time) *op.AtomicOp[definition.Branch] {
	// Move entity from the active to the deleted prefix
	var entity definition.Branch
	return op.Atomic(r.client, &entity).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&entity)).
		// Mark deleted
		BeforeWrite(func(ctx context.Context) {
			entity.Delete(now, false)
		}).
		// Save
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			return r.save(ctx, now, []definition.Branch{entity})
		})
}

func (r *Repository) Undelete(k key.BranchKey, now time.Time) *op.AtomicOp[definition.Branch] {
	// Move entity from the deleted to the active prefix
	var entity definition.Branch
	return op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.checkMaxBranchesPerProject(k.ProjectID, 1)).
		// Read the entity
		ReadOp(r.GetDeleted(k).WithResultTo(&entity)).
		// Mark undeleted
		BeforeWrite(func(ctx context.Context) {
			entity.Undelete(now)
		}).
		// Save
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			return r.save(ctx, now, []definition.Branch{entity})
		})
}

func (r *Repository) save(ctx context.Context, now time.Time, all []definition.Branch) (op.Op, error) {
	saveCtx := plugin2.NewSaveContext(now)
	for _, v := range all {
		// Call plugins
		r.plugins.Executor().OnBranchSave(saveCtx, &v)

		if v.Deleted {
			// Move entity from the active prefix to the deleted prefix
			saveCtx.AddOp(
				// Delete entity from the active prefix
				r.schema.Active().ByKey(v.BranchKey).Delete(r.client),
				// Save entity to the deleted prefix
				r.schema.Deleted().ByKey(v.BranchKey).Put(r.client, v),
			)
		} else {
			// Save record to the "active" prefix
			saveCtx.AddOp(r.schema.Active().ByKey(v.BranchKey).Put(r.client, v))

			if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(now) {
				// Delete record from the "deleted" prefix, if needed
				saveCtx.AddOp(r.schema.Deleted().ByKey(v.BranchKey).Delete(r.client))
			}
		}
	}

	return saveCtx.Apply(ctx)
}

func (r *Repository) checkMaxBranchesPerProject(k keboola.ProjectID, newCount int64) op.Op {
	return r.schema.
		Active().InProject(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxBranchesPerProject {
				return serviceError.NewCountLimitReachedError("branch", MaxBranchesPerProject, "project")
			}
			return nil
		})
}
