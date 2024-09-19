package mapper

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/mitchellh/mapstructure"

	streamDesign "github.com/keboola/keboola-as-code/api/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type taskOutputs struct {
	URL       *string            `mapstructure:"url,omitempty"`
	ProjectID *keboola.ProjectID `mapstructure:"projectId,omitempty"`
	BranchID  *keboola.BranchID  `mapstructure:"branchId,omitempty"`
	SourceID  *key.SourceID      `mapstructure:"sourceId,omitempty"`
	SinkID    *key.SinkID        `mapstructure:"sinkId,omitempty"`
}

func (m *Mapper) NewTaskResponse(entity task.Task) (*api.Task, error) {
	response := &api.Task{
		TaskID: entity.TaskID,
		Type:   entity.Type,
		URL:    m.formatTaskURL(entity.Key),
	}

	// Timestamps
	response.CreatedAt = entity.CreatedAt.String()
	if entity.FinishedAt != nil {
		v := entity.FinishedAt.String()
		response.FinishedAt = &v
	}
	if entity.Duration != nil {
		v := entity.Duration.Milliseconds()
		response.Duration = &v
	}

	// Status
	switch {
	case entity.IsProcessing():
		response.Status = streamDesign.TaskStatusProcessing
	case entity.IsSuccessful():
		response.Status = streamDesign.TaskStatusSuccess
		response.IsFinished = true
		response.Result = &entity.Result
	case entity.IsFailed():
		response.Status = streamDesign.TaskStatusError
		response.IsFinished = true
		response.Error = &entity.UserError.Message
	default:
		panic(errors.New("unexpected task status"))
	}

	// Outputs
	if entity.Outputs != nil {
		response.Outputs = &api.TaskOutputs{}
		err := mapstructure.Decode(entity.Outputs, response.Outputs)
		if err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (m *Mapper) WithTaskOutputs(result task.Result, v any) task.Result {
	outputs := taskOutputs{}

	switch v := v.(type) {
	case key.BranchKey:
		url := m.apiPublicURL.JoinPath("v1", "branches", v.BranchID.String()).String()
		outputs.URL = &url
		outputs.ProjectID = &v.ProjectID
		outputs.BranchID = &v.BranchID
	case key.SourceKey:
		url := m.apiPublicURL.JoinPath("v1", "branches", v.BranchID.String(), "sources", v.SourceID.String()).String()
		outputs.URL = &url
		outputs.ProjectID = &v.ProjectID
		outputs.BranchID = &v.BranchID
		outputs.SourceID = &v.SourceID
	case key.SinkKey:
		url := m.apiPublicURL.JoinPath("v1", "branches", v.BranchID.String(), "sources", v.SourceID.String(), "sinks", v.SinkID.String()).String()
		outputs.URL = &url
		outputs.ProjectID = &v.ProjectID
		outputs.BranchID = &v.BranchID
		outputs.SourceID = &v.SourceID
		outputs.SinkID = &v.SinkID
	}

	return result.WithOutputsFrom(outputs)
}

func (m *Mapper) formatTaskURL(k task.Key) string {
	return m.apiPublicURL.JoinPath("v1", "tasks", k.TaskID.String()).String()
}
