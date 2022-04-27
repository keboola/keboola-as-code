package service

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func RepositoriesResponse(v []model.TemplateRepository) *Repositories {
	out := &Repositories{}
	for _, repoRef := range getRepositories() {
		out.Repositories = append(out.Repositories, RepositoryResponse(repoRef))
	}
	return out
}

func RepositoryResponse(v model.TemplateRepository) *Repository {
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

func TemplatesResponse(repo *repository.Repository, templates []repository.TemplateRecord) *Templates {
	out := &Templates{Repository: RepositoryResponse(repo.Ref()), Templates: make([]*Template, 0)}
	for _, tmpl := range templates {
		tmpl := tmpl
		out.Templates = append(out.Templates, TemplateResponse(&tmpl, out.Repository.Author))
	}
	return out
}

func TemplateResponse(tmpl *repository.TemplateRecord, author *Author) *Template {
	defaultVersion, _ := tmpl.DefaultVersion()
	return &Template{
		ID:             tmpl.Id,
		Icon:           tmpl.Icon,
		Name:           tmpl.Name,
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         author,
	}
}

func TemplateDetailResponse(repo *repository.Repository, tmpl *repository.TemplateRecord) *TemplateDetail {
	defaultVersion, _ := tmpl.DefaultVersion()
	repoResponse := RepositoryResponse(repo.Ref())
	out := &TemplateDetail{
		Repository:     repoResponse,
		ID:             tmpl.Id,
		Icon:           tmpl.Icon,
		Name:           tmpl.Name,
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         repoResponse.Author,
		Versions:       make([]*TemplateVersion, 0),
	}
	for _, version := range tmpl.Versions {
		version := version
		out.Versions = append(out.Versions, VersionResponse(&version))
	}
	return out
}

func VersionResponse(v *repository.VersionRecord) *TemplateVersion {
	return &TemplateVersion{
		Version:     v.Version.String(),
		Stable:      v.Stable,
		Description: v.Description,
	}
}

func VersionDetailResponse(template *template.Template) *TemplateVersionDetail {
	repoResponse := RepositoryResponse(template.Repository())
	tmplRec := template.TemplateRecord()
	versionRec := template.VersionRecord()
	return &TemplateVersionDetail{
		Repository:  repoResponse,
		Template:    TemplateResponse(&tmplRec, repoResponse.Author),
		Version:     versionRec.Version.String(),
		Stable:      versionRec.Stable,
		Description: versionRec.Description,
		Components:  template.Components(),
		Readme:      template.Readme(),
	}
}

func InputsResponse(template *template.Template) (out *Inputs) {
	out = &Inputs{StepGroups: make([]*StepGroup, 0)}

	// Groups
	for _, group := range template.Inputs().ToExtended() {
		// Group
		groupResponse := &StepGroup{
			ID:          group.Id,
			Description: group.Description,
			Required:    group.Required,
			Steps:       make([]*Step, 0),
		}
		out.StepGroups = append(out.StepGroups, groupResponse)

		// Steps
		for _, step := range group.Steps {
			// Step
			stepResponse := &Step{
				ID:                step.Id,
				Icon:              step.Icon,
				Name:              step.Name,
				Description:       step.Description,
				DialogName:        step.NameFoDialog(),
				DialogDescription: step.DescriptionForDialog(),
				Inputs:            make([]*Input, 0),
			}
			groupResponse.Steps = append(groupResponse.Steps, stepResponse)

			// Inputs
			for _, in := range step.Inputs {
				inputResponse := &Input{
					ID:          in.Id,
					Name:        in.Name,
					Description: in.Description,
					Type:        string(in.Type),
					Kind:        string(in.Kind),
					Default:     in.DefaultOrEmpty(),
					Options:     OptionsResponse(in.Options),
				}
				stepResponse.Inputs = append(stepResponse.Inputs, inputResponse)
			}
		}
	}

	return out
}

func OptionsResponse(options input.Options) (out []*InputOption) {
	for _, opt := range options {
		out = append(out, &InputOption{
			Label: opt.Label,
			Value: opt.Value,
		})
	}
	return out
}
