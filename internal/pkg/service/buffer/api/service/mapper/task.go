package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func (m Mapper) TaskPayload(model *model.Task) (r *buffer.Task) {
	finishedAt := ""
	if model.FinishedAt != nil {
		finishedAt = model.FinishedAt.String()
	}
	return &buffer.Task{
		ID:         model.TaskID,
		ReceiverID: model.ReceiverID,
		URL:        formatTaskURL(m.bufferAPIHost, model.TaskKey),
		Type:       model.Type,
		CreatedAt:  model.CreatedAt.String(),
		FinishedAt: finishedAt,
		Duration:   model.Duration.Milliseconds(),
		IsFinished: model.IsFinished(),
		Result:     model.Result,
		Error:      model.Error,
	}
}
