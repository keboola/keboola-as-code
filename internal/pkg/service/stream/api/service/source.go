package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func (s *service) CreateSource(_ context.Context, d dependencies.BranchRequestScope, payload *api.CreateSourcePayload) (res *api.Task, err error) {
	source, err := s.mapper.NewSourceEntity(d.BranchKey(), payload)
	if err != nil {
		return nil, err
	}

	// Create source in a task
	t, err := s.startTask(taskConfig{
		Type:      "create.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: source.SourceKey,
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.repo.Source().Create(s.clock.Now(), "New source.", &source).Do(ctx).Err(); err == nil {
				result := task.OkResult("Source has been created successfully.")
				result = s.mapper.WithTaskOutputs(result, source.SourceKey)
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) UpdateSource(_ context.Context, d dependencies.SourceRequestScope, payload *api.UpdateSourcePayload) (res *api.Task, err error) {
	// Get the change description
	var changeDesc string
	if payload.ChangeDescription == nil {
		changeDesc = "Updated."
	} else {
		changeDesc = *payload.ChangeDescription
	}

	// Define update function
	update := func(source definition.Source) (definition.Source, error) {
		return s.mapper.UpdateSourceEntity(source, payload)
	}

	// Update source in a task
	t, err := s.startTask(taskConfig{
		Type:      "update.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.repo.Source().Update(s.clock.Now(), d.SourceKey(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Source has been updated successfully.")
				result = s.mapper.WithTaskOutputs(result, d.SourceKey())
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) ListSources(ctx context.Context, d dependencies.BranchRequestScope, payload *api.ListSourcesPayload) (res *api.SourcesList, err error) {
	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
		return s.repo.Source().List(d.BranchKey(), opts...)
	}
	return s.mapper.NewSourcesResponse(ctx, d.BranchKey(), payload.SinceID, payload.Limit, list)
}

func (s *service) GetSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.GetSourcePayload) (res *api.Source, err error) {
	source, err := s.repo.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSourceResponse(source), nil
}

func (s *service) DeleteSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.DeleteSourcePayload) (err error) {
	return s.repo.Source().SoftDelete(s.clock.Now(), d.SourceKey()).Do(ctx).Err()
}

func (s *service) GetSourceSettings(ctx context.Context, d dependencies.SourceRequestScope, _ *api.GetSourceSettingsPayload) (res *api.SettingsResult, err error) {
	source, err := s.repo.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) UpdateSourceSettings(ctx context.Context, d dependencies.SourceRequestScope, payload *api.UpdateSourceSettingsPayload) (res *api.SettingsResult, err error) {
	// Get the change description
	var changeDesc string
	if payload.ChangeDescription == nil {
		changeDesc = "Updated settings."
	} else {
		changeDesc = *payload.ChangeDescription
	}

	// Prepare patch KVs
	patch, err := s.mapper.NewSettingsPatch(payload.Settings, false)
	if err != nil {
		return nil, err
	}

	// Define update function
	update := func(source definition.Source) (definition.Source, error) {
		source.Config = source.Config.With(patch)
		return source, err
	}

	// Save changes
	source, err := s.repo.Source().Update(s.clock.Now(), d.SourceKey(), changeDesc, update).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) RefreshSourceTokens(context.Context, dependencies.SourceRequestScope, *api.RefreshSourceTokensPayload) (res *api.Source, err error) {
	return nil, errors.NewNotImplementedError()
}
