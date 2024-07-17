package service

import (
	"context"
	"net/http"
	"path"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

type service struct {
	config       config.Config
	deps         dependencies.ServiceScope
	proxyHandler http.Handler
}

func New(ctx context.Context, d dependencies.ServiceScope) (Service, error) {
	proxyHandler := proxy.NewHandler(d)
	s := &service{
		config:       d.Config(),
		deps:         d,
		proxyHandler: proxyHandler,
	}

	return s, nil
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (res *ServiceDetail, err error) {
	url := *s.deps.Config().API.PublicURL
	url.Path = path.Join(url.Path, "v1/documentation")
	res = &ServiceDetail{
		API:           "templates",
		Documentation: url.String(),
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (res string, err error) {
	return "OK", nil
}

func (s *service) Validate(context.Context, dependencies.ProjectRequestScope, *ValidatePayload) (res *Validations, err error) {
	return
}

func (s *service) Proxy(ctx context.Context, d dependencies.PublicRequestScope) (res any, err error) {
	request, _ := middleware.RequestValue(ctx)
	w := middleware.ResponseWriter(ctx)
	s.proxyHandler.ServeHTTP(w, request)
	return
}

func (s *service) ProxyPath(ctx context.Context, d dependencies.PublicRequestScope, pr *ProxyRequest) (res any, err error) {
	return s.Proxy(ctx, d)
}
