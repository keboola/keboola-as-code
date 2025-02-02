package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	ComponentsTemplateRepositoryName     = "keboola-components"
	ComponentsTemplateRepositoryNameBeta = "keboola-components-beta"

	ComponentsTemplateRepositoryURL = "https://github.com/keboola/keboola-as-code-templates-components.git"

	FeatureComponentsTemplateRepositoryBeta = "components-repository-beta"
)

func ComponentsRepository() model.TemplateRepository {
	return model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: ComponentsTemplateRepositoryName,
		URL:  ComponentsTemplateRepositoryURL,
		Ref:  DefaultTemplateRepositoryRefMain,
	}
}
