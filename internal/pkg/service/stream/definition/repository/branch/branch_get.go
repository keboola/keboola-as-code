package branch

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) Get(k key.BranchKey) op.WithResult[definition.Branch] {
	return r.schema.
		Active().ByKey(k).GetOrErr(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("branch", k.BranchID.String(), "project")
		})
}

func (r *Repository) GetDeleted(k key.BranchKey) op.WithResult[definition.Branch] {
	return r.schema.
		Deleted().ByKey(k).GetOrErr(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted branch", k.BranchID.String(), "project")
		})
}

func (r *Repository) GetDefault(k keboola.ProjectID) *op.TxnOp[definition.Branch] {
	found := false
	var entity definition.Branch
	return op.
		TxnWithResult(r.client, &entity).
		Then(r.List(k).ForEach(func(branch definition.Branch, _ *iterator.Header) error {
			if branch.IsDefault {
				found = true
				entity = branch
			}
			return nil
		})).
		OnSucceeded(func(r *op.TxnResult[definition.Branch]) {
			if !found {
				r.AddErr(serviceError.NewResourceNotFoundError("branch", "default", "project"))
			}
		})
}
