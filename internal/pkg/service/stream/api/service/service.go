package service

import (
	"context"
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/mapper"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

type service struct {
	publicURL *url.URL
	repo      *definitionRepo.Repository
	mapper    *mapper.Mapper
}

func New(d dependencies.APIScope) stream.Service {
	return &service{
		publicURL: d.APIPublicURL(),
		repo:      d.DefinitionRepository(),
		mapper:    mapper.New(d),
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

func (s *service) GetTask(context.Context, dependencies.BranchRequestScope, *stream.GetTaskPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}
