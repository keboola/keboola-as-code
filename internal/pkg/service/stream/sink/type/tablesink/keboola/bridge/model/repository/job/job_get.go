package job

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
)

// Get returns job by key.
func (r *Repository) Get(k key.JobKey) op.WithResult[model.Job] {
	return r.schema.ByKey(k).GetOrErr(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("job", k.String(), "sink")
		})
}
