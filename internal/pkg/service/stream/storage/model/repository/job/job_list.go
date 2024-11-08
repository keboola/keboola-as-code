package job

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/job/schema"
)

func (r *Repository) ListAll() iterator.DefinitionT[model.Job] {
	return r.schema.Active().GetAll(r.client)
}

func (r *Repository) List(parentKey any, opts ...iterator.Option) iterator.DefinitionT[model.Job] {
	return r.list(r.schema.Active(), parentKey, opts...)
}

func (r *Repository) list(pfx schema.JobInState, parentKey any, opts ...iterator.Option) iterator.DefinitionT[model.Job] {
	return pfx.In(parentKey).GetAll(r.client, opts...)
}
