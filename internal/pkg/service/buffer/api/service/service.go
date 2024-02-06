package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

type service struct{}

func New(d dependencies.APIScope) stream.Service {
	return &service{}
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (res *stream.ServiceDetail, err error) {
	res = &stream.ServiceDetail{
		API:           "buffer",
		Documentation: "https://stream.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (res string, err error) {
	return "OK", nil
}

func (s *service) CreateSource(context.Context, dependencies.ProjectRequestScope, *stream.CreateSourcePayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSource(context.Context, dependencies.ProjectRequestScope, *stream.UpdateSourcePayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSources(context.Context, dependencies.ProjectRequestScope, *stream.ListSourcesPayload) (res *stream.SourcesList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSource(context.Context, dependencies.ProjectRequestScope, *stream.GetSourcePayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSource(context.Context, dependencies.ProjectRequestScope, *stream.DeleteSourcePayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSourceSettings(context.Context, dependencies.ProjectRequestScope, *stream.GetSourceSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSourceSettings(context.Context, dependencies.ProjectRequestScope, *stream.UpdateSourceSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) RefreshSourceTokens(context.Context, dependencies.ProjectRequestScope, *stream.RefreshSourceTokensPayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) CreateSink(context.Context, dependencies.ProjectRequestScope, *stream.CreateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSink(context.Context, dependencies.ProjectRequestScope, *stream.GetSinkPayload) (res *stream.Sink, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSinks(context.Context, dependencies.ProjectRequestScope, *stream.ListSinksPayload) (res *stream.SinksList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSink(context.Context, dependencies.ProjectRequestScope, *stream.UpdateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSink(context.Context, dependencies.ProjectRequestScope, *stream.DeleteSinkPayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSinkSettings(context.Context, dependencies.ProjectRequestScope, *stream.GetSinkSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSinkSettings(context.Context, dependencies.ProjectRequestScope, *stream.UpdateSinkSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetTask(context.Context, dependencies.ProjectRequestScope, *stream.GetTaskPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}
