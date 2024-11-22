package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	ComponentsTemplateRepositoryName    = "keboola-components"
	ComponentsTemplateRepositoryNameDev = "keboola-components-dev"

	ComponentsTemplateRepositoryURL = "https://github.com/keboola/keboola-as-code-templates-components.git"

	FeatureComponentsTemplateRepositoryDev = "components-repository-dev"
)

func ComponentsRepository() model.TemplateRepository {
	return model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: ComponentsTemplateRepositoryName,
		URL:  ComponentsTemplateRepositoryURL,
		Ref:  DefaultTemplateRepositoryRefMain,
	}
}
