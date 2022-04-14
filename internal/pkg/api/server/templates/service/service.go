package service

import (
	"errors"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
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
	return RepositoriesResponse(getRepositories()), nil
}

func (s *service) RepositoryIndex(_ dependencies.Container, payload *RepositoryIndexPayload) (res *Repository, err error) {
	repoRef, err := getRepositoryRef(payload.Repository)
	if err != nil {
		return nil, err
	}
	return RepositoryResponse(repoRef), nil
}

func (s *service) TemplatesIndex(d dependencies.Container, payload *TemplatesIndexPayload) (res *Templates, err error) {
	repo, err := getRepository(d, payload.Repository)
	if err != nil {
		return nil, err
	}
	return TemplatesResponse(repo, repo.Templates()), nil
}

func (s *service) TemplateIndex(d dependencies.Container, payload *TemplateIndexPayload) (res *TemplateDetail, err error) {
	repo, tmplRecord, err := getTemplateRecord(d, payload.Repository, payload.Template)
	if err != nil {
		return nil, err
	}
	return TemplateDetailResponse(repo, tmplRecord), nil
}

func (s *service) VersionIndex(d dependencies.Container, payload *VersionIndexPayload) (res *TemplateVersionDetail, err error) {
	tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return VersionDetailResponse(tmpl), nil
}

func (s *service) InputsIndex(d dependencies.Container, payload *InputsIndexPayload) (res *Inputs, err error) {
	tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return InputsResponse(tmpl), nil
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

func getRepositories() []model.TemplateRepository {
	return []model.TemplateRepository{
		{
			Type: "git",
			Name: repository.DefaultTemplateRepositoryName,
			Url:  repository.DefaultTemplateRepositoryUrl,
			Ref:  "api-demo",
			Author: model.TemplateAuthor{
				Name: "Keboola",
				Url:  "https://www.keboola.com",
			},
		},
	}
}

func getRepositoryRef(name string) (model.TemplateRepository, error) {
	for _, repo := range getRepositories() {
		if repo.Name == name {
			return repo, nil
		}
	}
	return model.TemplateRepository{}, &GenericError{
		Name:    "templates.repositoryNotFound",
		Message: fmt.Sprintf(`Repository "%s" not found.`, name),
	}
}

func getRepository(d dependencies.Container, repoName string) (*repository.Repository, error) {
	repoRef, err := getRepositoryRef(repoName)
	if err != nil {
		return nil, err
	}
	repo, err := d.TemplateRepository(repoRef, nil)
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func getTemplateRecord(d dependencies.Container, repoName, templateId string) (*repository.Repository, *repository.TemplateRecord, error) {
	repo, err := getRepository(d, repoName)
	if err != nil {
		return nil, nil, err
	}
	tmpl, found := repo.GetTemplateById(templateId)
	if !found {
		return nil, nil, &GenericError{
			Name:    "templates.templateNotFound",
			Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
		}
	}
	return repo, &tmpl, nil
}

func getTemplateVersion(d dependencies.Container, repoName, templateId, versionStr string) (*template.Template, error) {
	// Get repository ref
	repoRef, err := getRepositoryRef(repoName)
	if err != nil {
		return nil, err
	}

	// Parse version
	semVersion, err := model.NewSemVersion(versionStr)
	if err != nil {
		return nil, &BadRequestError{
			Message: fmt.Sprintf(`Version "%s" is not valid: %s`, versionStr, err),
		}
	}

	tmpl, err := d.Template(model.NewTemplateRef(repoRef, templateId, semVersion))
	if err != nil {
		if errors.As(err, &manifest.TemplateNotFoundError{}) {
			return nil, &GenericError{
				Name:    "templates.templateNotFound",
				Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
			}
		}
		if errors.As(err, &manifest.VersionNotFoundError{}) {
			return nil, &GenericError{
				Name:    "templates.versionNotFound",
				Message: fmt.Sprintf(`Version "%s" not found.`, versionStr),
			}
		}
	}

	return tmpl, nil
}
