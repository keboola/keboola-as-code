package branch

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	etcd "go.etcd.io/etcd/client/v3"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
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

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Branch) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Do(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *definition.Branch) {
	// Call plugins
	r.plugins.Executor().OnBranchSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.WriteOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.BranchKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.BranchKey).Put(r.client, *updated),
		)
	} else {
		// Save record to the "active" prefix
		saveCtx.WriteOp(r.schema.Active().ByKey(updated.BranchKey).Put(r.client, *updated))

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(saveCtx.Now()) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.WriteOp(r.schema.Deleted().ByKey(updated.BranchKey).Delete(r.client))
		}
	}
}
