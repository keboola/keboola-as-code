package job

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) ExistsOrErr(k key.JobKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.sinks.ExistsOrErr(k.SinkKey)).
		Merge(r.schema.ByKey(k).Exists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("job", k.JobID.String(), "sink")
			}),
		).
		FirstErrorOnly()
}

func (r *Repository) MustNotExist(k key.JobKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.sinks.ExistsOrErr(k.SinkKey)).
		Merge(r.schema.ByKey(k).Exists(r.client).
			WithNotEmptyResultAsError(func() error {
				return serviceError.NewResourceAlreadyExistsError("job", k.JobID.String(), "sink")
			}),
		).
		FirstErrorOnly()
}