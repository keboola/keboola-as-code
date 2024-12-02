package job

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
)

func (r *Repository) ExistsOrErr(k model.JobKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.sinks.ExistsOrErr(k.SinkKey)).
		Merge(r.schema.ByKey(k).Exists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("job", k.JobID.String(), "sink")
			}),
		).
		FirstErrorOnly()
}

func (r *Repository) MustNotExist(k model.JobKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.sinks.ExistsOrErr(k.SinkKey)).
		Merge(r.schema.ByKey(k).Exists(r.client).
			WithNotEmptyResultAsError(func() error {
				return serviceError.NewResourceAlreadyExistsError("job", k.JobID.String(), "sink")
			}),
		).
		FirstErrorOnly()
}
