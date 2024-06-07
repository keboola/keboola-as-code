package sink

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) Get(k key.SinkKey) op.WithResult[definition.Sink] {
	return r.schema.
		Active().ByKey(k).GetOrErr(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
		})
}

func (r *Repository) GetDeleted(k key.SinkKey) op.WithResult[definition.Sink] {
	return r.schema.
		Deleted().ByKey(k).GetOrErr(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted sink", k.SinkID.String(), "source")
		})
}
