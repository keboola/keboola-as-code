package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func (s *service) GetTask(d dependencies.ForProjectRequest, payload *buffer.GetTaskPayload) (res *buffer.Task, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	t, err := str.GetTask(ctx, key.TaskKey{
		ProjectID: key.ProjectID(d.ProjectID()),
		TaskID:    payload.TaskID,
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.TaskPayload(&t), nil
}
