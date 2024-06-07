package file

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// Get file entity.
func (r *Repository) Get(k model.FileKey) op.WithResult[model.File] {
	return r.schema.AllLevels().ByKey(k).GetOrErr(r.client).WithEmptyResultAsError(func() error {
		return serviceError.NewResourceNotFoundError("file", k.String(), "sink")
	})
}
