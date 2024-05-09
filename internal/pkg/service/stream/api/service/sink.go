package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

//nolint:dupl // CreateSource method is similar
func (s *service) CreateSink(ctx context.Context, d dependencies.SourceRequestScope, payload *stream.CreateSinkPayload) (res *stream.Task, err error) {
	sink, err := s.mapper.NewSinkEntity(d.SourceKey(), payload)
	if err != nil {
		return nil, err
	}

	// Create sink in a task
	t, err := s.startTask(taskConfig{
		Type:      "create.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: sink.SinkKey,
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.repo.Sink().Create(&sink, s.clock.Now(), d.RequestUser(), "New sink.").Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been created successfully.")
				result = s.mapper.WithTaskOutputs(result, sink.SinkKey)
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

func (s *service) GetSink(ctx context.Context, d dependencies.SinkRequestScope, _ *stream.GetSinkPayload) (res *stream.Sink, err error) {
	sink, err := s.repo.Sink().Get(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSinkResponse(sink)
}

func (s *service) ListSinks(ctx context.Context, d dependencies.SourceRequestScope, payload *stream.ListSinksPayload) (res *stream.SinksList, err error) {
	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Sink] {
		return s.repo.Sink().List(d.SourceKey(), opts...)
	}
	return s.mapper.NewSinksResponse(ctx, d.SourceKey(), payload.SinceID, payload.Limit, list)
}

func (s *service) UpdateSink(ctx context.Context, d dependencies.SinkRequestScope, payload *stream.UpdateSinkPayload) (res *stream.Task, err error) {
	// Get the change description
	var changeDesc string
	if payload.ChangeDescription == nil {
		changeDesc = "Updated."
	} else {
		changeDesc = *payload.ChangeDescription
	}

	// Define update function
	update := func(sink definition.Sink) (definition.Sink, error) {
		return s.mapper.UpdateSinkEntity(sink, payload)
	}

	// Update sink in a task
	t, err := s.startTask(taskConfig{
		Type:      "update.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.repo.Sink().Update(d.SinkKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been updated successfully.")
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

func (s *service) DeleteSink(ctx context.Context, d dependencies.SinkRequestScope, _ *stream.DeleteSinkPayload) (err error) {
	return s.repo.Sink().SoftDelete(d.SinkKey(), s.clock.Now(), d.RequestUser()).Do(ctx).Err()
}

func (s *service) GetSinkSettings(ctx context.Context, d dependencies.SinkRequestScope, _ *stream.GetSinkSettingsPayload) (res *stream.SettingsResult, err error) {
	source, err := s.repo.Sink().Get(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) UpdateSinkSettings(ctx context.Context, d dependencies.SinkRequestScope, payload *stream.UpdateSinkSettingsPayload) (res *stream.SettingsResult, err error) {
	rb := rollback.New(d.Logger())
	defer rb.InvokeIfErr(ctx, &err)

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
	update := func(sink definition.Sink) (definition.Sink, error) {
		sink.Config = sink.Config.With(patch)
		return sink, err
	}

	// Save changes
	source, err := s.repo.Sink().Update(d.SinkKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) SinkStatisticsTotal(ctx context.Context, d dependencies.SinkRequestScope, payload *stream.SinkStatisticsTotalPayload) (res *stream.SinkStatisticsTotalResult, err error) {
	err = s.repo.Sink().ExistsOrErr(d.SinkKey()).Do(ctx).Err()
	if err != nil {
		return nil, err
	}

	stats, err := d.StatisticsRepository().SinkStats(ctx, d.SinkKey())
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSinkStatisticsTotalResponse(stats), nil
}

func (s *service) SinkStatisticsFiles(context.Context, dependencies.SinkRequestScope, *stream.SinkStatisticsFilesPayload) (res *stream.SinkStatisticsFilesResult, err error) {
	return nil, errors.NewNotImplementedError()
}
