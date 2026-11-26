package service

import (
	"context"
	"net/url"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

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

// Project admin roles allowed to modify streams.
// The "share" role is the "admin" role plus bucket sharing.
const (
	adminRole = "admin"
	shareRole = "share"
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

// canWrite reports whether the token may create, modify or delete streams.
// Only master tokens of project admins qualify, readOnly and guest roles are refused.
func canWrite(token keboola.Token) bool {
	return token.Admin != nil && (token.Admin.Role == adminRole || token.Admin.Role == shareRole)
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
