package service

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
)

type service struct {
	deps     dependencies.APIScope
	clock    clock.Clock
	logger   log.Logger
	schema   *schema.Schema
	mapper   *mapper.Mapper
	importer *receive.Importer
}

func New(d dependencies.APIScope) buffer.Service {
	return &service{
		deps:     d,
		clock:    d.Clock(),
		logger:   d.Logger(),
		schema:   d.Schema(),
		mapper:   mapper.NewMapper(d),
		importer: receive.NewImporter(d),
	}
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (res *buffer.ServiceDetail, err error) {
	res = &buffer.ServiceDetail{
		API:           "buffer",
		Documentation: "https://buffer.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (res string, err error) {
	return "OK", nil
}
