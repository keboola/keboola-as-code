package service

import (
	"context"
	"path"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
)

type service struct {
	config config.Config
	deps   dependencies.ServiceScope
}

func New(ctx context.Context, d dependencies.ServiceScope) (Service, error) {
	s := &service{
		config: d.Config(),
		deps:   d,
	}

	return s, nil
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) (err error) {
	// Redirect /_proxy/api -> /_proxy/api/v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (res *ServiceDetail, err error) {
	url := *s.deps.Config().API.PublicURL
	url.Path = path.Join(url.Path, "v1/documentation")
	res = &ServiceDetail{
		API:           "apps-proxy",
		Documentation: url.String(),
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (res string, err error) {
	return "OK", nil
}

func (s *service) Validate(context.Context, dependencies.ProjectRequestScope, *ValidatePayload) (res *Validations, err error) {
	return res, err
}
