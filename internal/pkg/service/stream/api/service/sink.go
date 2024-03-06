package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func (s *service) CreateSink(_ context.Context, d dependencies.SourceRequestScope, payload *stream.CreateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSink(context.Context, dependencies.SinkRequestScope, *stream.GetSinkPayload) (res *stream.Sink, err error) {
	return nil, errors.NewNotImplementedError()
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
