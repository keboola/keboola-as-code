package branch

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

const (
	MaxBranchesPerProject = 100
)

type Repository struct {
	client  etcd.KV
	plugins *plugin.Plugins
	schema  schema.Branch
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
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
	var created definition.Branch
	var actual, deleted *op.KeyValueT[definition.Branch]

	return op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.checkMaxBranchesPerProject(k.ProjectID, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// Entity must not exist
			if actual != nil {
				return nil, serviceError.NewResourceAlreadyExistsError("branch", k.BranchID.String(), "project")
			}

			// Create or undelete
			created = deepcopy.Copy(*input).(definition.Branch)
			if deleted != nil {
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now)
			}

			// Save
			return r.saveOne(ctx, now, nil, &created)
		}).
		// Update the input entity after a successful operation
		OnResult(func(entity definition.Branch) {
			*input = entity
		})
}

func (r *Repository) SoftDelete(k key.BranchKey, now time.Time) *op.AtomicOp[definition.Branch] {
	// Move entity from the active to the deleted prefix
	var old, updated definition.Branch
	return op.Atomic(r.client, &updated).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Mark deleted
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			updated = deepcopy.Copy(old).(definition.Branch)
			updated.Delete(now, false)
			return r.saveOne(ctx, now, &old, &updated)
		})
}

func (r *Repository) Undelete(k key.BranchKey, now time.Time) *op.AtomicOp[definition.Branch] {
	// Move entity from the deleted to the active prefix
	var created definition.Branch
	return op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.checkMaxBranchesPerProject(k.ProjectID, 1)).
		// Read the entity
		ReadOp(r.GetDeleted(k).WithResultTo(&created)).
		// Mark undeleted
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			created.Undelete(now)
			return r.saveOne(ctx, now, nil, &created)
		})
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Branch) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, now, old, updated)
	return saveCtx.Apply(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, now time.Time, old, updated *definition.Branch) {
	// Call plugins
	r.plugins.Executor().OnBranchSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.AddOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.BranchKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.BranchKey).Put(r.client, *updated),
		)
	} else {
		// Save record to the "active" prefix
		saveCtx.AddOp(r.schema.Active().ByKey(updated.BranchKey).Put(r.client, *updated))

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(now) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.AddOp(r.schema.Deleted().ByKey(updated.BranchKey).Delete(r.client))
		}
	}
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
