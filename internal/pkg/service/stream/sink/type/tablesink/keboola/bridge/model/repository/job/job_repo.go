package job

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/repository/job/schema"
)

type Repository struct {
	client  *etcd.Client
	schema  schema.Job
	sinks   *sink.Repository
	plugins *plugin.Plugins
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	DefinitionRepository() *definitionRepo.Repository
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies) *Repository {
	r := &Repository{
		client:  d.EtcdClient(),
		schema:  schema.New(d.EtcdSerde()),
		sinks:   d.DefinitionRepository().Sink(),
		plugins: d.Plugins(),
	}

	r.purgeJobsOnSinkDelete()
	return r
}

// save Job on create, triggers connected plugins to enrich the operation.
func (r *Repository) save(updated *model.Job) *op.TxnOp[model.Job] {
	// Call no plugins

	saveTxn := op.TxnWithResult(r.client, updated)
	if updated.Deleted {
		// Delete entity from the active prefix
		saveTxn.Then(
			r.schema.ByKey(updated.JobKey).Delete(r.client),
		)
	} else {
		// Save record to the "active" prefix
		saveTxn.Then(
			r.schema.ByKey(updated.JobKey).Put(r.client, *updated),
		)
	}

	return saveTxn
}
