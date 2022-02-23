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

func (s *TemplatesService) IndexEndpoint(_ context.Context) (res *templates.Index, err error) {
	apiName := "templates"
	documentationUrl := "https://templates.keboola.com/documentation"
	res = &templates.Index{
		API:           &apiName,
		Documentation: &documentationUrl,
	}
	s.logger.Print("templates.index")
	return res, nil
}

func (s *TemplatesService) HealthCheck(_ context.Context) (err error) {
	s.logger.Print("templates.health-check")
	return
}
