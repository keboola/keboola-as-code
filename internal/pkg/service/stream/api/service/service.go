package service

import (
	"context"
	"net/url"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

type service struct {
	clock      clock.Clock
	publicURL  *url.URL
	tasks      *task.Node
	definition *definitionRepo.Repository
	mapper     *mapper.Mapper
}

func New(d dependencies.APIScope, cfg config.Config) api.Service {
	return &service{
		clock:      d.Clock(),
		publicURL:  d.APIPublicURL(),
		tasks:      d.TaskNode(),
		definition: d.DefinitionRepository(),
		mapper:     mapper.New(d, cfg),
	}
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) error {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (*api.ServiceDetail, error) {
	return &api.ServiceDetail{
		API:           "stream",
		Documentation: s.publicURL.JoinPath("v1", "documentation").String(),
	}, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (string, error) {
	return "OK", nil
}
