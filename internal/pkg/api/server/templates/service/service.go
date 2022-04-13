package service

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type service struct{}

func New(d dependencies.Container) (Service, error) {
	if err := StartPullCron(d); err != nil {
		return nil, err
	}
	return &service{}, nil
}

func (s *service) APIRootIndex(dependencies.Container) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(dependencies.Container) (res *ServiceDetail, err error) {
	res = &ServiceDetail{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(dependencies.Container) (res string, err error) {
	return "OK", nil
}

func (s *service) RepositoriesIndex(dependencies.Container, *RepositoriesIndexPayload) (res *Repositories, err error) {
	out := &Repositories{}
	for _, repo := range projectRepositories() {
		out.Repositories = append(out.Repositories, MapRepository(repo))
	}
	return out, nil
}

func (s *service) RepositoryIndex(_ dependencies.Container, payload *RepositoryIndexPayload) (res *Repository, err error) {
	repo, found := projectRepository(payload.Repository)
	if !found {
		return nil, &GenericError{
			Name:    "RepositoryNotFound",
			Message: fmt.Sprintf(`Repository "%s" not found.`, payload.Repository),
		}
	}
	return MapRepository(repo), nil
}

func (s *service) TemplatesIndex(dependencies.Container, *TemplatesIndexPayload) (res *Templates, err error) {
	return nil, NotImplementedError{}
}

func (s *service) TemplateIndex(dependencies.Container, *TemplateIndexPayload) (res *TemplateDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) VersionIndex(dependencies.Container, *VersionIndexPayload) (res *TemplateVersionDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) InputsIndex(dependencies.Container, *InputsIndexPayload) (res *Inputs, err error) {
	return nil, NotImplementedError{}
}

func (s *service) ValidateInputs(dependencies.Container, *ValidateInputsPayload) (res *ValidationResult, err error) {
	return nil, NotImplementedError{}
}

func (s *service) UseTemplateVersion(dependencies.Container, *UseTemplateVersionPayload) (res *UseTemplateDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) InstancesIndex(dependencies.Container, *InstancesIndexPayload) (err error) {
	return NotImplementedError{}
}

func (s *service) InstanceIndex(dependencies.Container, *InstanceIndexPayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpdateInstance(dependencies.Container, *UpdateInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) DeleteInstance(dependencies.Container, *DeleteInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstance(dependencies.Container, *UpgradeInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstanceInputsIndex(dependencies.Container, *UpgradeInstanceInputsIndexPayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstanceValidateInputs(dependencies.Container, *UpgradeInstanceValidateInputsPayload) (err error) {
	return NotImplementedError{}
}

func projectRepository(name string) (model.TemplateRepository, bool) {
	for _, repo := range projectRepositories() {
		if repo.Name == name {
			return repo, true
		}
	}
	return model.TemplateRepository{}, false
}

func projectRepositories() []model.TemplateRepository {
	return []model.TemplateRepository{repository.DefaultRepository()}
}
