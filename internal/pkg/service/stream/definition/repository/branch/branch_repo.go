package branch

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
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

// save Branch on create or update, trigger connected plugins to enrich the operation.
func (r *Repository) save(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) op.Op {
	// Call plugins
	r.plugins.Executor().OnBranchSave(ctx, now, by, old, updated)

	saveTxn := op.Txn(r.client)
	if updated.IsDeleted() {
		// Move entity from the active prefix to the deleted prefix
		saveTxn.Then(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.BranchKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.BranchKey).Put(r.client, *updated),
		)
	} else {
		// Save record to the "active" prefix
		saveTxn.Then(r.schema.Active().ByKey(updated.BranchKey).Put(r.client, *updated))

		if updated.IsUndeletedAt(now) {
			// Delete record from the "deleted" prefix, if needed
			saveTxn.Then(r.schema.Deleted().ByKey(updated.BranchKey).Delete(r.client))
		}
	}

	return saveTxn
}

func (r *Repository) update(k key.BranchKey, now time.Time, by definition.By, updateFn func(definition.Branch) (definition.Branch, error)) *op.AtomicOp[definition.Branch] {
	var old, updated definition.Branch
	return op.Atomic(r.client, &updated).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			updated = deepcopy.Copy(old).(definition.Branch)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			return r.save(ctx, now, by, &old, &updated), nil
		})
}
