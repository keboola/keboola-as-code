package service

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func MapRepositoryRef(v model.TemplateRepository) *Repository {
	return &Repository{
		Name: v.Name,
		URL:  v.Url,
		Ref:  v.Ref,
		Author: &Author{
			Name: v.Author.Name,
			URL:  v.Author.Url,
		},
	}
}

func MapTemplate(v repository.TemplateRecord, author *Author) *Template {
	defaultVersion, _ := v.DefaultVersion()
	return &Template{
		ID:             v.Id,
		Icon:           "todo",
		Name:           v.Name,
		Description:    v.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         author,
	}
}
