package service

import (
	"context"
	"fmt"

	"goa.design/goa/v3/security"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
)

type TemplatesService struct {
	dependencies dependencies.Container
}

func New(d dependencies.Container) templates.Service {
	return &TemplatesService{dependencies: d}
}

func (s *TemplatesService) APIKeyAuth(ctx context.Context, _ string, _ *security.APIKeyScheme) (context.Context, error) {
	return ctx, nil
}

func (s *TemplatesService) IndexRoot(_ dependencies.Container) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *TemplatesService) HealthCheck(_ dependencies.Container) (res string, err error) {
	return "OK", nil
}

func (s *TemplatesService) IndexEndpoint(_ dependencies.Container) (res *templates.Index, err error) {
	res = &templates.Index{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *TemplatesService) Foo(_ dependencies.Container, payload *templates.FooPayload) (res string, err error) {
	s.dependencies.Logger().Infof("dependencies work!")
	return fmt.Sprintf("token length: %d\n", len(payload.StorageAPIToken)), nil
}
