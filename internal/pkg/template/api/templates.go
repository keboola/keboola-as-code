package api

import (
	"context"
	"log"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
)

type TemplatesService struct {
	logger *log.Logger
}

func NewTemplates(logger *log.Logger) templates.Service {
	return &TemplatesService{logger}
}

func (s *TemplatesService) IndexRoot(context.Context) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *TemplatesService) HealthCheck(_ context.Context) (err error) {
	return
}

func (s *TemplatesService) IndexEndpoint(_ context.Context) (res *templates.Index, err error) {
	res = &templates.Index{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}
