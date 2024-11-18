package job

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/repository/job/schema"
)

func (r *Repository) ListAll() iterator.DefinitionT[model.Job] {
	return r.schema.GetAll(r.client)
}

func (r *Repository) List(parentKey any, opts ...iterator.Option) iterator.DefinitionT[model.Job] {
	return r.list(r.schema, parentKey, opts...)
}

func (r *Repository) list(pfx schema.Job, parentKey any, opts ...iterator.Option) iterator.DefinitionT[model.Job] {
	return pfx.In(parentKey).GetAll(r.client, opts...)
}
