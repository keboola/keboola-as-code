package branch

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	etcd "go.etcd.io/etcd/client/v3"
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

func (r *Repository) save(ctx context.Context, old, updated *definition.Branch) {
	// Call plugins
	r.plugins.Executor().OnBranchSave(ctx, old, updated)

	pluginOp := plugin.FromContext(ctx)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		pluginOp.WriteOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.BranchKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.BranchKey).Put(r.client, *updated),
		)
	} else {
		// Save record to the "active" prefix
		pluginOp.WriteOp(r.schema.Active().ByKey(updated.BranchKey).Put(r.client, *updated))

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(pluginOp.Now()) {
			// Delete record from the "deleted" prefix, if needed
			pluginOp.WriteOp(r.schema.Deleted().ByKey(updated.BranchKey).Delete(r.client))
		}
	}
}
