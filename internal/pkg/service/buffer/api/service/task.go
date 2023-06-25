package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

func (s *service) GetTask(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.GetTaskPayload) (res *buffer.Task, err error) {
	t, err := d.Store().GetTask(ctx, task.Key{
		ProjectID: d.ProjectID(),
		TaskID:    payload.TaskID,
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.TaskPayload(&t), nil
}
