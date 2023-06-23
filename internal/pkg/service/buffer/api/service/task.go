package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

func (s *service) GetTask(d dependencies.ProjectRequestScope, payload *buffer.GetTaskPayload) (res *buffer.Task, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	t, err := str.GetTask(ctx, task.Key{
		ProjectID: d.ProjectID(),
		TaskID:    payload.TaskID,
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.TaskPayload(&t), nil
}
