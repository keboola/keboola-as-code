package store

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

func (s *Store) GetTask(taskKey task.Key) op.ForType[*op.KeyValueT[task.Task]] {
	return s.schema.
		Tasks().
		ByKey(taskKey).
		Get(s.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("task", taskKey.TaskID.String(), "project")
		})
}
