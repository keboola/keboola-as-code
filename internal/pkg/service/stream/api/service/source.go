package service

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func (s *service) CreateSource(_ context.Context, d dependencies.BranchRequestScope, payload *stream.CreateSourcePayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSource(context.Context, dependencies.SourceRequestScope, *stream.UpdateSourcePayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSources(context.Context, dependencies.BranchRequestScope, *stream.ListSourcesPayload) (res *stream.SourcesList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSource(context.Context, dependencies.SourceRequestScope, *stream.GetSourcePayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSource(context.Context, dependencies.SourceRequestScope, *stream.DeleteSourcePayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSourceSettings(context.Context, dependencies.SourceRequestScope, *stream.GetSourceSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSourceSettings(context.Context, dependencies.SourceRequestScope, *stream.UpdateSourceSettingsPayload) (res stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) RefreshSourceTokens(context.Context, dependencies.SourceRequestScope, *stream.RefreshSourceTokensPayload) (res *stream.Source, err error) {
	return nil, errors.NewNotImplementedError()
}
