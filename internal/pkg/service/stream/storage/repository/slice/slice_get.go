package slice

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// Get slice entity.
func (r *Repository) Get(k model.SliceKey) op.WithResult[model.Slice] {
	return r.schema.AllLevels().ByKey(k).Get(r.client).WithEmptyResultAsError(func() error {
		return serviceError.NewResourceNotFoundError("slice", k.String(), "file")
	})
}
