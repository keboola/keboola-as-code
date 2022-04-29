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

func (s *service) RepositoriesIndex(d dependencies.Container, _ *RepositoriesIndexPayload) (res *Repositories, err error) {
	return RepositoriesResponse(d, getRepositories())
}

func (s *service) RepositoryIndex(d dependencies.Container, payload *RepositoryIndexPayload) (res *Repository, err error) {
	repo, err := getRepository(d, payload.Repository)
	if err != nil {
		return nil, err
	}
	return RepositoryResponse(repo), nil
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

func (s *service) VersionIndex(d dependencies.Container, payload *VersionIndexPayload) (res *VersionDetailExtended, err error) {
	repo, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return VersionDetailResponse(repo, tmpl), nil
}

func (s *service) InputsIndex(d dependencies.Container, payload *InputsIndexPayload) (res *Inputs, err error) {
	_, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return InputsResponse(tmpl), nil
}

func (s *service) ValidateInputs(d dependencies.Container, payload *ValidateInputsPayload) (res *ValidationResult, err error) {
	_, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return validateInputs(tmpl.Inputs(), payload.Steps)
}

func (s *service) UseTemplateVersion(d dependencies.Container, payload *UseTemplateVersionPayload) (res *UseTemplateResult, err error) {
	_, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	result, err := validateInputs(tmpl.Inputs(), payload.Steps)
	if err != nil {
		return nil, err
	}
	if !result.Valid {
		return nil, &ValidationError{
			Name:             "InvalidInputs",
			Message:          "Inputs are not valid.",
			ValidationResult: result,
		}
	}

	return nil, NotImplementedError{}
}

func (s *service) InstancesIndex(dependencies.Container, *InstancesIndexPayload) (res *Instances, err error) {
	return nil, NotImplementedError{}
}

func (s *service) InstanceIndex(dependencies.Container, *InstanceIndexPayload) (res *InstanceDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) UpdateInstance(dependencies.Container, *UpdateInstancePayload) (res *InstanceDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) DeleteInstance(dependencies.Container, *DeleteInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstance(dependencies.Container, *UpgradeInstancePayload) (res *UpgradeInstanceResult, err error) {
	return nil, NotImplementedError{}
}

func (s *service) UpgradeInstanceInputsIndex(dependencies.Container, *UpgradeInstanceInputsIndexPayload) (res *Inputs, err error) {
	return nil, NotImplementedError{}
}

func (s *service) UpgradeInstanceValidateInputs(dependencies.Container, *UpgradeInstanceValidateInputsPayload) (res *ValidationResult, err error) {
	return nil, NotImplementedError{}
}

func getRepositories() []model.TemplateRepository {
	return []model.TemplateRepository{
		{
			Type: "git",
			Name: repository.DefaultTemplateRepositoryName,
			Url:  repository.DefaultTemplateRepositoryUrl,
			Ref:  "api-demo",
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
	// Get repository ref
	repoRef, err := getRepositoryRef(repoName)
	if err != nil {
		return nil, err
	}

	// Get repository
	repo, err := d.TemplateRepository(repoRef, nil)
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func getTemplateRecord(d dependencies.Container, repoName, templateId string) (*repository.Repository, *repository.TemplateRecord, error) {
	// Get repository
	repo, err := getRepository(d, repoName)
	if err != nil {
		return nil, nil, err
	}

	// Get template record
	tmpl, found := repo.GetTemplateById(templateId)
	if !found {
		return nil, nil, &GenericError{
			Name:    "templates.templateNotFound",
			Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
		}
	}
	return repo, &tmpl, nil
}

func getTemplateVersion(d dependencies.Container, repoName, templateId, versionStr string) (*repository.Repository, *template.Template, error) {
	// Parse version
	semVersion, err := model.NewSemVersion(versionStr)
	if err != nil {
		return nil, nil, &BadRequestError{
			Message: fmt.Sprintf(`Version "%s" is not valid: %s`, versionStr, err),
		}
	}

	repo, err := getRepository(d, repoName)
	if err != nil {
		return nil, nil, err
	}

	tmpl, err := d.Template(model.NewTemplateRef(repo.Ref(), templateId, semVersion))
	if err != nil {
		if errors.As(err, &manifest.TemplateNotFoundError{}) {
			return nil, nil, &GenericError{
				Name:    "templates.templateNotFound",
				Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
			}
		}
		if errors.As(err, &manifest.VersionNotFoundError{}) {
			return nil, nil, &GenericError{
				Name:    "templates.versionNotFound",
				Message: fmt.Sprintf(`Version "%s" not found.`, versionStr),
			}
		}
		return nil, nil, err
	}

	return repo, tmpl, nil
}
