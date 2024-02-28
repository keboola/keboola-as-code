package service

import (
	"context"
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

type service struct {
	publicURL *url.URL
	repo      *definitionRepo.Repository
}

func New(d dependencies.APIScope) stream.Service {
	return &service{
		publicURL: d.APIPublicURL(),
		repo:      d.DefinitionRepository(),
	}
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (res *stream.ServiceDetail, err error) {
	res = &stream.ServiceDetail{
		API:           "stream",
		Documentation: s.publicURL.JoinPath("v1", "documentation").String(),
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (res string, err error) {
	return "OK", nil
}

func (s *service) CreateSource(context.Context, dependencies.BranchRequestScope, *stream.CreateSourcePayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSource(context.Context, dependencies.BranchRequestScope, *stream.UpdateSourcePayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSources(context.Context, dependencies.BranchRequestScope, *stream.ListSourcesPayload) (res *stream.SourcesList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSource(context.Context, dependencies.BranchRequestScope, *stream.GetSourcePayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSource(context.Context, dependencies.BranchRequestScope, *stream.DeleteSourcePayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSourceSettings(context.Context, dependencies.BranchRequestScope, *stream.GetSourceSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSourceSettings(context.Context, dependencies.BranchRequestScope, *stream.UpdateSourceSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) RefreshSourceTokens(context.Context, dependencies.BranchRequestScope, *stream.RefreshSourceTokensPayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) CreateSink(context.Context, dependencies.BranchRequestScope, *stream.CreateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSink(context.Context, dependencies.BranchRequestScope, *stream.GetSinkPayload) (res *stream.Sink, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSinks(context.Context, dependencies.BranchRequestScope, *stream.ListSinksPayload) (res *stream.SinksList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSink(context.Context, dependencies.BranchRequestScope, *stream.UpdateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSink(context.Context, dependencies.BranchRequestScope, *stream.DeleteSinkPayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSinkSettings(context.Context, dependencies.BranchRequestScope, *stream.GetSinkSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSinkSettings(context.Context, dependencies.BranchRequestScope, *stream.UpdateSinkSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetTask(context.Context, dependencies.BranchRequestScope, *stream.GetTaskPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}
