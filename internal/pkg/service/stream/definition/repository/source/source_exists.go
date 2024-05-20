package source

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) ExistsOrErr(k key.SourceKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.branches.ExistsOrErr(k.BranchKey)).
		Merge(r.schema.Active().ByKey(k).Exists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch")
			}),
		).
		FirstErrorOnly()
}

func (r *Repository) MustNotExists(k key.SourceKey) *op.TxnOp[op.NoResult] {
	return op.Txn(r.client).
		Merge(r.branches.ExistsOrErr(k.BranchKey)).
		Merge(r.schema.Active().ByKey(k).Exists(r.client).
			WithNotEmptyResultAsError(func() error {
				return serviceError.NewResourceAlreadyExistsError("source", k.SourceID.String(), "branch")
			}),
		).
		FirstErrorOnly()
}
