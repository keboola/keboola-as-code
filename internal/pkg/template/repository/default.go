package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	DefaultTemplateRepositoryName    = "keboola"
	DefaultTemplateRepositoryUrl     = "https://github.com/keboola/keboola-as-code-templates.git"
	DefaultTemplateRepositoryRefBeta = "beta"
	DefaultTemplateRepositoryRefDev  = "dev"
	DefaultTemplateRepositoryRefMain = "main"

	FeatureTemplateRepositoryBeta = "templates-repository-beta"
	FeatureTemplateRepositoryDev  = "templates-repository-dev"
)

func DefaultRepository() model.TemplateRepository {
	return model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: DefaultTemplateRepositoryName,
		Url:  DefaultTemplateRepositoryUrl,
		Ref:  DefaultTemplateRepositoryRefMain,
	}
}
