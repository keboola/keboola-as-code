package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	DefaultTemplateRepositoryName = "keboola"
	DefaultTemplateRepositoryUrl  = "git@github.com:keboola/keboola-as-code-templates.git"
	DefaultTemplateRepositoryRef  = "main"
)

func DefaultRepository() model.TemplateRepository {
	return model.TemplateRepository{
		Type: "git",
		Name: DefaultTemplateRepositoryName,
		Url:  DefaultTemplateRepositoryUrl,
		Ref:  DefaultTemplateRepositoryRef,
	}
}
