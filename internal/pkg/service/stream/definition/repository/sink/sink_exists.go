package sink

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) ExistsOrErr(k key.SinkKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.sources.ExistsOrErr(k.SourceKey)).
		Merge(r.schema.Active().ByKey(k).Exists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
			}),
		).
		FirstErrorOnly()
}

func (r *Repository) MustNotExists(k key.SinkKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.sources.ExistsOrErr(k.SourceKey)).
		Merge(r.schema.Active().ByKey(k).Exists(r.client).
			WithNotEmptyResultAsError(func() error {
				return serviceError.NewResourceAlreadyExistsError("sink", k.SinkID.String(), "source")
			}),
		).
		FirstErrorOnly()
}
