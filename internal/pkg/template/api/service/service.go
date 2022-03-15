package service

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
)

type Service struct {
	dependencies dependencies.Container
}

func New(d dependencies.Container) templates.Service {
	return &Service{dependencies: d}
}

func (s *Service) IndexRoot(_ dependencies.Container) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *Service) HealthCheck(_ dependencies.Container) (res string, err error) {
	return "OK", nil
}

func (s *Service) IndexEndpoint(_ dependencies.Container) (res *templates.Index, err error) {
	res = &templates.Index{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *Service) Foo(d dependencies.Container, payload *templates.FooPayload) (res string, err error) {
	api, err := d.StorageApi()
	if err != nil {
		return "", err
	}

	token := api.Token()
	msg := fmt.Sprintf("token is OK, owner=%s", token.Owner.Name)

	d.Logger().Info(msg)
	return msg, nil
}
