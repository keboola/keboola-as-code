package service

import (
	"context"
	"net/url"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	adminRole = "admin"
)

type service struct {
	logger     log.Logger
	clock      clockwork.Clock
	publicURL  *url.URL
	tasks      *task.Node
	locks      *distlock.Provider
	definition *definitionRepo.Repository
	mapper     *mapper.Mapper
	adminError error
}

func New(d dependencies.APIScope, cfg config.Config) api.Service {
	return &service{
		logger:     d.Logger(),
		clock:      d.Clock(),
		publicURL:  d.APIPublicURL(),
		tasks:      d.TaskNode(),
		locks:      d.DistributedLockProvider(),
		definition: d.DefinitionRepository(),
		mapper:     mapper.New(d, cfg),
		adminError: errors.New("only admin token can do write operations on streams"),
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
