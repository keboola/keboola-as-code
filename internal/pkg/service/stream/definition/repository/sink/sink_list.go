package sink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink/schema"
)

func (r *Repository) List(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *Repository) ListDeleted(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *Repository) list(pfx schema.SinkInState, parentKey any) iterator.DefinitionT[definition.Sink] {
	return pfx.In(parentKey).GetAll(r.client)
}
