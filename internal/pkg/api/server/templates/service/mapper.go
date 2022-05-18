package service

import (
	"fmt"
	"time"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
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

func VersionDetailResponse(template *template.Template) *VersionDetail {
	versionRec := template.VersionRecord()
	return &VersionDetail{
		Version:     versionRec.Version.String(),
		Stable:      versionRec.Stable,
		Description: versionRec.Description,
		Components:  template.Components(),
		Readme:      template.Readme(),
	}
}

func VersionDetailExtendedResponse(repo *repository.Repository, template *template.Template) *VersionDetailExtended {
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
			Required:    string(group.Required),
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

func InstancesResponse(prjState *project.State, branchKey model.BranchKey) (out *Instances, err error) {
	// Get branch state
	branch, found := prjState.GetOrNil(branchKey).(*model.BranchState)
	if !found {
		return nil, &GenericError{
			Name:    "templates.branchNotFound",
			Message: fmt.Sprintf(`Branch "%d" not found.`, branchKey.Id),
		}
	}

	// Get instances
	instances, err := branch.Remote.Metadata.TemplatesUsages()
	if err != nil {
		return nil, err
	}

	// Map response
	out = &Instances{Instances: make([]*Instance, 0)}
	for _, instance := range instances {
		outInstance := &Instance{
			TemplateID:     instance.TemplateId,
			InstanceID:     instance.InstanceId,
			Branch:         cast.ToString(branch.Id),
			RepositoryName: instance.RepositoryName,
			Version:        instance.Version,
			Name:           instance.InstanceName,
			Created: &ChangeInfo{
				Date:    instance.Created.Date.Format(time.RFC3339),
				TokenID: instance.Created.TokenId,
			},
			Updated: &ChangeInfo{
				Date:    instance.Updated.Date.Format(time.RFC3339),
				TokenID: instance.Updated.TokenId,
			},
		}

		if instance.MainConfig != nil {
			configKey := model.ConfigKey{BranchId: branchKey.Id, ComponentId: instance.MainConfig.ComponentId, Id: instance.MainConfig.ConfigId}
			if _, found := prjState.Get(configKey); found {
				outInstance.MainConfig = &MainConfig{
					ComponentID: string(instance.MainConfig.ComponentId),
					ConfigID:    string(instance.MainConfig.ConfigId),
				}
			}
		}

		out.Instances = append(out.Instances, outInstance)
	}

	return out, nil
}

func InstanceResponse(d dependencies.Container, prjState *project.State, branchKey model.BranchKey, instanceId string) (out *InstanceDetail, err error) {
	// Get branch state
	branch, found := prjState.GetOrNil(branchKey).(*model.BranchState)
	if !found {
		return nil, &GenericError{
			Name:    "templates.branchNotFound",
			Message: fmt.Sprintf(`Branch "%d" not found.`, branchKey.Id),
		}
	}

	// Get instances
	instance, found, err := branch.Remote.Metadata.TemplateUsage(instanceId)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, &GenericError{
			Name:    "templates.instanceNotFound",
			Message: fmt.Sprintf(`Instance "%s" not found in branch "%d".`, instanceId, branchKey.Id),
		}
	}

	// Map configurations
	outConfigs := make([]*Config, 0)
	configs := search.ConfigsForTemplateInstance(prjState.RemoteObjects().ConfigsWithRowsFrom(branchKey), instanceId)
	for _, config := range configs {
		outConfigs = append(outConfigs, &Config{
			Name:        config.Name,
			ConfigID:    string(config.Id),
			ComponentID: string(config.ComponentId),
		})
	}

	// Map response
	out = &InstanceDetail{
		VersionDetail:  instanceVersionDetail(d, instance),
		TemplateID:     instance.TemplateId,
		InstanceID:     instance.InstanceId,
		Branch:         cast.ToString(branch.Id),
		RepositoryName: instance.RepositoryName,
		Version:        instance.Version,
		Name:           instance.InstanceName,
		Created: &ChangeInfo{
			Date:    instance.Created.Date.Format(time.RFC3339),
			TokenID: instance.Created.TokenId,
		},
		Updated: &ChangeInfo{
			Date:    instance.Updated.Date.Format(time.RFC3339),
			TokenID: instance.Updated.TokenId,
		},
		Configurations: outConfigs,
	}

	// Main config
	if instance.MainConfig != nil {
		configKey := model.ConfigKey{BranchId: branchKey.Id, ComponentId: instance.MainConfig.ComponentId, Id: instance.MainConfig.ConfigId}
		if _, found := prjState.Get(configKey); found {
			out.MainConfig = &MainConfig{
				ComponentID: string(instance.MainConfig.ComponentId),
				ConfigID:    string(instance.MainConfig.ConfigId),
			}
		}
	}

	return out, nil
}

func instanceVersionDetail(d dependencies.Container, instance *model.TemplateUsageRecord) *VersionDetail {
	repo, tmplRecord, err := templateRecord(d, instance.RepositoryName, instance.TemplateId)
	if err != nil {
		return nil
	}
	semVer, err := model.NewSemVersion(instance.Version)
	if err != nil {
		return nil
	}
	versionRecord, found := tmplRecord.GetClosestVersion(semVer)
	if !found {
		return nil
	}
	tmpl, err := d.Template(model.NewTemplateRef(repo.Ref(), instance.TemplateId, versionRecord.Version))
	if err != nil {
		return nil
	}
	return VersionDetailResponse(tmpl)
}
