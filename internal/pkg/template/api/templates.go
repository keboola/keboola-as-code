package api

import (
	"context"
	"fmt"
	"log"

	"goa.design/goa/v3/security"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
)

type TemplatesService struct {
	logger *log.Logger
}

func NewTemplates(logger *log.Logger) templates.Service {
	return &TemplatesService{logger}
}

func (s *TemplatesService) APIKeyAuth(ctx context.Context, _ string, _ *security.APIKeyScheme) (context.Context, error) {
	return ctx, nil
}

func (s *TemplatesService) IndexRoot(_ context.Context) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *TemplatesService) HealthCheck(_ context.Context) (res string, err error) {
	return "OK", nil
}

func (s *TemplatesService) IndexEndpoint(_ context.Context) (res *templates.Index, err error) {
	res = &templates.Index{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *TemplatesService) Foo(_ context.Context, payload *templates.FooPayload) (res string, err error) {
	return fmt.Sprintf("token length: %d\n", len(payload.StorageAPIToken)), nil
}
