package service

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func MapRepository(v model.TemplateRepository) *Repository {
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
