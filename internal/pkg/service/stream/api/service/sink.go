package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
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
			if err := s.repo.Sink().Create(&sink, s.clock.Now(), "New sink.").Do(ctx).Err(); err == nil {
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

func (s *service) ListSinks(context.Context, dependencies.SourceRequestScope, *stream.ListSinksPayload) (res *stream.SinksList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSink(context.Context, dependencies.SinkRequestScope, *stream.UpdateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSink(context.Context, dependencies.SinkRequestScope, *stream.DeleteSinkPayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSinkSettings(context.Context, dependencies.SinkRequestScope, *stream.GetSinkSettingsPayload) (res *stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSinkSettings(context.Context, dependencies.SinkRequestScope, *stream.UpdateSinkSettingsPayload) (res *stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}
