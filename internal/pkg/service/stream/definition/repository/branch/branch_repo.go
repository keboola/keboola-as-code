package branch

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
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

func (r *Repository) save(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) op.Op {
	// Call plugins
	r.plugins.Executor().OnBranchSave(ctx, now, by, old, updated)

	saveTxn := op.Txn(r.client)
	if updated.Deleted {
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

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(now) {
			// Delete record from the "deleted" prefix, if needed
			saveTxn.Then(r.schema.Deleted().ByKey(updated.BranchKey).Delete(r.client))
		}
	}

	return saveTxn
}
