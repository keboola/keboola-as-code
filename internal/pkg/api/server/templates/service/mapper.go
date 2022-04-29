package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func RepositoriesResponse(d dependencies.Container, v []model.TemplateRepository) (*Repositories, error) {
	out := &Repositories{}
	for _, repoRef := range v {
		repo, err := repositoryInst(d, repoRef.Name)
		if err != nil {
			return nil, err
		}
		out.Repositories = append(out.Repositories, RepositoryResponse(repo))
	}
	return out, nil
}

func RepositoryResponse(v *repository.Repository) *Repository {
	ref := v.Ref()
	author := v.Manifest().Author()
	return &Repository{
		Name: ref.Name,
		URL:  ref.Url,
		Ref:  ref.Ref,
		Author: &Author{
			Name: author.Name,
			URL:  author.Url,
		},
	}
}

func TemplatesResponse(repo *repository.Repository, templates []repository.TemplateRecord) *Templates {
	out := &Templates{Repository: RepositoryResponse(repo), Templates: make([]*Template, 0)}
	for _, tmpl := range templates {
		tmpl := tmpl
		out.Templates = append(out.Templates, TemplateResponse(&tmpl, out.Repository.Author))
	}
	return out
}

func TemplateResponse(tmpl *repository.TemplateRecord, author *Author) *Template {
	defaultVersion, _ := tmpl.DefaultVersion()
	out := &Template{
		ID:             tmpl.Id,
		Icon:           tmpl.Icon,
		Name:           tmpl.Name,
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         author,
		Versions:       make([]*Version, 0),
	}

	for _, version := range tmpl.Versions {
		version := version
		out.Versions = append(out.Versions, VersionResponse(&version))
	}
	return out
}

func TemplateDetailResponse(repo *repository.Repository, tmpl *repository.TemplateRecord) *TemplateDetail {
	defaultVersion, _ := tmpl.DefaultVersion()
	repoResponse := RepositoryResponse(repo)
	out := &TemplateDetail{
		Repository:     repoResponse,
		ID:             tmpl.Id,
		Icon:           tmpl.Icon,
		Name:           tmpl.Name,
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         repoResponse.Author,
		Versions:       make([]*Version, 0),
	}
	for _, version := range tmpl.Versions {
		version := version
		out.Versions = append(out.Versions, VersionResponse(&version))
	}
	return out
}

func VersionResponse(v *repository.VersionRecord) *Version {
	return &Version{
		Version:     v.Version.String(),
		Stable:      v.Stable,
		Description: v.Description,
	}
}

func VersionDetailResponse(repo *repository.Repository, template *template.Template) *VersionDetailExtended {
	repoResponse := RepositoryResponse(repo)
	tmplRec := template.TemplateRecord()
	versionRec := template.VersionRecord()
	return &VersionDetailExtended{
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
