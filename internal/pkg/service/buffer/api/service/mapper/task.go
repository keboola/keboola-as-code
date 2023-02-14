package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func (m Mapper) TaskPayload(model *model.Task) (r *buffer.Task) {
	var finishedAtPtr *string
	if model.FinishedAt != nil {
		finishedAtStr := model.FinishedAt.String()
		finishedAtPtr = &finishedAtStr
	}
	var durationMs int64 = 0
	if model.Duration != nil {
		durationMs = model.Duration.Milliseconds()
	}
	var errPtr *string
	if model.Error != "" {
		errPtr = &model.Error
	}
	return &buffer.Task{
		ID:         model.TaskID,
		ReceiverID: model.ReceiverID,
		URL:        formatTaskURL(m.bufferAPIHost, model.TaskKey),
		Type:       model.Type,
		CreatedAt:  model.CreatedAt.String(),
		FinishedAt: finishedAtPtr,
		Duration:   durationMs,
		IsFinished: model.IsFinished(),
		Result:     model.Result,
		Error:      errPtr,
	}
}
