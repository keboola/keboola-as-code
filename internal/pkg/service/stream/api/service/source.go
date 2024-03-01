package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func (s *service) CreateSource(_ context.Context, d dependencies.BranchRequestScope, payload *api.CreateSourcePayload) (res *api.Task, err error) {
	source, err := s.mapper.NewSourceEntity(d.BranchKey(), payload)
	if err != nil {
		return nil, err
	}

	t, err := s.startTask(taskConfig{
		Type:      "create.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: source.SourceKey,
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.repo.Source().Create("New source.", &source).Do(ctx).Err(); err == nil {
				return s.mapper.WithTaskOutputs(task.OkResult("Source has been created successfully."), source.SourceKey)
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t), nil
}

func (s *service) UpdateSource(context.Context, dependencies.SourceRequestScope, *api.UpdateSourcePayload) (res *api.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSources(context.Context, dependencies.BranchRequestScope, *api.ListSourcesPayload) (res *api.SourcesList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.GetSourcePayload) (res *api.Source, err error) {
	source, err := s.repo.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSourceResponse(source), nil
}

func (s *service) DeleteSource(context.Context, dependencies.SourceRequestScope, *api.DeleteSourcePayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSourceSettings(context.Context, dependencies.SourceRequestScope, *api.GetSourceSettingsPayload) (res api.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSourceSettings(context.Context, dependencies.SourceRequestScope, *api.UpdateSourceSettingsPayload) (res api.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) RefreshSourceTokens(context.Context, dependencies.SourceRequestScope, *api.RefreshSourceTokensPayload) (res *api.Source, err error) {
	return nil, errors.NewNotImplementedError()
}
