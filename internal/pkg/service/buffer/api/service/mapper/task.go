package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func (m Mapper) TaskPayload(model *model.Task) (r *buffer.Task) {
	var finishedAtPtr *string
	if model.FinishedAt != nil {
		v := model.FinishedAt.String()
		finishedAtPtr = &v
	}

	var resultPtr *string
	var errorPtr *string
	if model.Error == "" {
		resultPtr = &model.Result
	} else {
		errorPtr = &model.Error
	}

	var durationMsPtr *int64
	if model.Duration != nil {
		v := model.Duration.Milliseconds()
		durationMsPtr = &v
	}

	return &buffer.Task{
		ID:         model.TaskID,
		ReceiverID: model.ReceiverID,
		URL:        formatTaskURL(m.bufferAPIHost, model.TaskKey),
		Type:       model.Type,
		CreatedAt:  model.CreatedAt.String(),
		FinishedAt: finishedAtPtr,
		Duration:   durationMsPtr,
		IsFinished: model.IsFinished(),
		Result:     resultPtr,
		Error:      errorPtr,
	}
}
