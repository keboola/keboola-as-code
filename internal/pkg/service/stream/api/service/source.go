package service

import (
	"context"
	"io"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
)

//nolint:dupl // CreateSink method is similar
func (s *service) CreateSource(ctx context.Context, d dependencies.BranchRequestScope, payload *api.CreateSourcePayload) (*api.Task, error) {
	// Create entity
	source, err := s.mapper.NewSourceEntity(d.BranchKey(), payload)
	if err != nil {
		return nil, err
	}

	// Quick check before the task
	if err := s.sourceMustNotExist(ctx, source.SourceKey); err != nil {
		return nil, err
	}

	// Create source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "create.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: source.SourceKey,
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Source().Create(&source, s.clock.Now(), d.RequestUser(), "New source.").Do(ctx).Err(); err == nil {
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

func (s *service) UpdateSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.UpdateSourcePayload) (*api.Task, error) {
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

	// Quick validation - without save and associated slow operations
	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	if _, err := update(source); err != nil {
		return nil, err
	}

	// Update source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.definition.Source().Update(d.SourceKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
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

func (s *service) ListSources(ctx context.Context, d dependencies.BranchRequestScope, payload *api.ListSourcesPayload) (*api.SourcesList, error) {
	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
		return s.definition.Source().List(d.BranchKey(), opts...)
	}
	return s.mapper.NewSourcesResponse(ctx, d.BranchKey(), payload.AfterID, payload.Limit, list)
}

func (s *service) GetSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.GetSourcePayload) (*api.Source, error) {
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSourceResponse(source)
}

func (s *service) DeleteSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.DeleteSourcePayload) (*api.Task, error) {
	// Quick check before the task
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Delete source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "delete.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Source().SoftDelete(d.SourceKey(), s.clock.Now(), d.RequestUser()).Do(ctx).Err(); err == nil {
				result := task.OkResult("Source has been deleted successfully.")
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

func (s *service) GetSourceSettings(ctx context.Context, d dependencies.SourceRequestScope, _ *api.GetSourceSettingsPayload) (*api.SettingsResult, error) {
	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) UpdateSourceSettings(ctx context.Context, d dependencies.SourceRequestScope, payload *api.UpdateSourceSettingsPayload) (*api.Task, error) {
	// Quick check before the task
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

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

	// Update source settings in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.sourceSettings",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.definition.Source().Update(d.SourceKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Source settings have been updated successfully.")
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

func (s *service) TestSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.TestSourcePayload, _ io.ReadCloser) (res *api.TestResult, err error) {
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	sinks, err := s.definition.Sink().List(d.SourceKey()).Do(ctx).All()
	if err != nil {
		return nil, err
	}

	// Remove X-StorageApi-Token from headers
	req := d.Request()
	req.Header.Del("x-storageapi-token")

	recordCtx := recordctx.FromHTTP(d.Clock().Now(), req)

	return s.mapper.NewTestResultResponse(d.SourceKey(), sinks, recordCtx)
}

func (s *service) SourceStatisticsClear(ctx context.Context, d dependencies.SourceRequestScope, payload *api.SourceStatisticsClearPayload) (err error) {
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return err
	}

	sinks, err := s.definition.Sink().List(d.SourceKey()).Do(ctx).All()
	if err != nil {
		return err
	}

	sinkKeys := []key.SinkKey{}
	for _, sink := range sinks {
		sinkKeys = append(sinkKeys, sink.SinkKey)
	}

	return d.StatisticsRepository().ResetAllSinksStats(ctx, sinkKeys)
}

func (s *service) DisableSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.DisableSourcePayload) (res *api.Task, err error) {
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Disable source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "disable.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Source().Disable(d.SourceKey(), d.Clock().Now(), d.RequestUser(), "API").Do(ctx).Err(); err == nil {
				result := task.OkResult("Source has been disabled successfully.")
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

func (s *service) EnableSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.EnableSourcePayload) (res *api.Task, err error) {
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Enable source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "enable.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Source().Enable(d.SourceKey(), d.Clock().Now(), d.RequestUser()).Do(ctx).Err(); err == nil {
				result := task.OkResult("Source has been enabled successfully.")
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

func (s *service) sourceMustNotExist(ctx context.Context, k key.SourceKey) error {
	return s.definition.Source().MustNotExists(k).Do(ctx).Err()
}

func (s *service) sourceMustExists(ctx context.Context, k key.SourceKey) error {
	return s.definition.Source().ExistsOrErr(k).Do(ctx).Err()
}
