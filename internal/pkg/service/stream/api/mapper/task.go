package mapper

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cast"

	streamDesign "github.com/keboola/keboola-as-code/api/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Mapper) NewTaskResponse(entity task.Task) *api.Task {
	response := &api.Task{
		ID:   entity.TaskID,
		Type: entity.Type,
		URL:  m.formatTaskURL(entity.Key),
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
		response.Error = &entity.Error
	default:
		panic(errors.New("unexpected task status"))
	}

	// Outputs
	if entity.Outputs != nil {
		response.Outputs = &api.TaskOutputs{}
		if v, ok := entity.Outputs["url"]; ok {
			url := cast.ToString(v)
			response.Outputs.URL = &url
		}
		if v, ok := entity.Outputs["projectId"]; ok {
			id := keboola.ProjectID(cast.ToInt(v))
			response.Outputs.ProjectID = &id
		}
		if v, ok := entity.Outputs["branchId"]; ok {
			id := keboola.BranchID(cast.ToInt(v))
			response.Outputs.BranchID = &id
		}
		if v, ok := entity.Outputs["sourceId"]; ok {
			id := key.SourceID(cast.ToString(v))
			response.Outputs.SourceID = &id
		}
		if v, ok := entity.Outputs["sinkId"]; ok {
			id := key.SinkID(cast.ToString(v))
			response.Outputs.SinkID = &id
		}
	}

	return response
}

func (m *Mapper) WithTaskOutputs(result task.Result, v any) task.Result {
	switch v := v.(type) {
	case key.BranchKey:
		return result.
			WithOutput("url", m.apiPublicURL.JoinPath("v1", "branches", v.BranchID.String()).String()).
			WithOutput("projectId", int(v.ProjectID)).
			WithOutput("branchId", int(v.BranchID))
	case key.SourceKey:
		return result.
			WithOutput("url", m.apiPublicURL.JoinPath("v1", "branches", v.BranchID.String(), "sources", v.SourceID.String()).String()).
			WithOutput("projectId", int(v.ProjectID)).
			WithOutput("branchId", int(v.BranchID)).
			WithOutput("sourceId", v.SourceID.String())
	case key.SinkKey:
		return result.
			WithOutput("url", m.apiPublicURL.JoinPath("v1", "branches", v.BranchID.String(), "sources", v.SourceID.String(), "sinks", v.SinkID.String()).String()).
			WithOutput("projectId", int(v.ProjectID)).
			WithOutput("branchId", int(v.BranchID)).
			WithOutput("sourceId", v.SourceID.String()).
			WithOutput("sinkId", v.SinkID.String())
	}

	return result
}

func (m *Mapper) formatTaskURL(k task.Key) string {
	return m.apiPublicURL.JoinPath("v1", "tasks", k.TaskID.String()).String()
}
