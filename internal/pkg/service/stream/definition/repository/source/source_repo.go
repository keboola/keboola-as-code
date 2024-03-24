package source

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	etcd "go.etcd.io/etcd/client/v3"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

type Repository struct {
	client   etcd.KV
	schema   schema.Source
	plugins  *plugin.Plugins
	branches *branch.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies, branches *branch.Repository) *Repository {
	r := &Repository{
		client:   d.EtcdClient(),
		schema:   schema.ForSource(d.EtcdSerde()),
		plugins:  d.Plugins(),
		branches: branches,
	}

	r.deleteSourcesOnBranchDelete()
	r.undeleteSourcesOnBranchUndelete()
	return r
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Source) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Do(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *definition.Source) {
	// Call plugins
	r.plugins.Executor().OnSourceSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.WriteOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.SourceKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.SourceKey).Put(r.client, *updated),
		)
	} else {
		saveCtx.WriteOp(
			// Save record to the "active" prefix
			r.schema.Active().ByKey(updated.SourceKey).Put(r.client, *updated),
			// Save record to the versions history
			r.schema.Versions().Of(updated.SourceKey).Version(updated.VersionNumber()).Put(r.client, *updated),
		)

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(saveCtx.Now()) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.WriteOp(r.schema.Deleted().ByKey(updated.SourceKey).Delete(r.client))
		}
	}
}
