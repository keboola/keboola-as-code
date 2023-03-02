package service

import (
	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
)

type service struct {
	deps     dependencies.ForServer
	clock    clock.Clock
	logger   log.Logger
	schema   *schema.Schema
	mapper   *mapper.Mapper
	importer *receive.Importer
}

func New(d dependencies.ForServer) buffer.Service {
	return &service{
		deps:     d,
		clock:    d.Clock(),
		logger:   d.Logger(),
		schema:   d.Schema(),
		mapper:   mapper.NewMapper(d),
		importer: receive.NewImporter(d),
	}
}

func (s *service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(dependencies.ForPublicRequest) (res *buffer.ServiceDetail, err error) {
	res = &buffer.ServiceDetail{
		API:           "buffer",
		Documentation: "https://buffer.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(dependencies.ForPublicRequest) (res string, err error) {
	return "OK", nil
}
