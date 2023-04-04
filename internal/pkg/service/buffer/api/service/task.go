package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	commonKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/key"
	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
)

func (s *service) GetTask(d dependencies.ForProjectRequest, payload *buffer.GetTaskPayload) (res *buffer.Task, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	t, err := str.GetTask(ctx, taskKey.Key{
		ProjectID: commonKey.ProjectID(d.ProjectID()),
		TaskID:    payload.TaskID,
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.TaskPayload(&t), nil
}
