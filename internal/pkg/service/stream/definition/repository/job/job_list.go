package job

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/job/schema"
)

func (r *Repository) List(parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Job] {
	return r.list(r.schema.Active(), parentKey, opts...)
}

func (r *Repository) ListDeleted(parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Job] {
	return r.list(r.schema.Deleted(), parentKey, opts...)
}

func (r *Repository) list(pfx schema.JobInState, parentKey any, opts ...iterator.Option) iterator.DefinitionT[definition.Job] {
	return pfx.In(parentKey).GetAll(r.client, opts...)
}
