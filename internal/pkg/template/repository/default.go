package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	DefaultTemplateRepositoryName     = "keboola"
	DefaultTemplateRepositoryNameBeta = "keboola-beta"
	DefaultTemplateRepositoryNameDev  = "keboola-dev"

	DefaultTemplateRepositoryURL     = "https://github.com/keboola/keboola-as-code-templates.git"
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
		URL:  DefaultTemplateRepositoryURL,
		Ref:  DefaultTemplateRepositoryRefMain,
	}
}
