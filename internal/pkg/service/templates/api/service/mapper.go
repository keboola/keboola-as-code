package service

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

func RepositoriesResponse(ctx context.Context, d dependencies.ForProjectRequest) (out *Repositories, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.RepositoriesResponse")
	defer telemetry.EndSpan(span, &err)

	out = &Repositories{}
	for _, repoRef := range d.ProjectRepositories().All() {
		repo, err := repositoryInst(d, repoRef.Name)
		if err != nil {
			return nil, err
		}
		out.Repositories = append(out.Repositories, RepositoryResponse(ctx, d, repo))
	}
	return out, nil
}

func RepositoryResponse(ctx context.Context, d dependencies.ForProjectRequest, v *repository.Repository) *Repository {
	_, span := d.Tracer().Start(ctx, "api.server.templates.mapper.RepositoryResponse")
	defer telemetry.EndSpan(span, nil)

	repo := v.Definition()
	author := v.Manifest().Author()
	return &Repository{
		Name: repo.Name,
		URL:  repo.URL,
		Ref:  repo.Ref,
		Author: &Author{
			Name: author.Name,
			URL:  author.URL,
		},
	}
}

func TemplatesResponse(ctx context.Context, d dependencies.ForProjectRequest, repo *repository.Repository, templates []repository.TemplateRecord) (out *Templates, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.TemplatesResponse")
	defer telemetry.EndSpan(span, &err)

	out = &Templates{Repository: RepositoryResponse(ctx, d, repo), Templates: make([]*Template, 0)}
	for _, tmpl := range templates {
		tmpl := tmpl
		tmplResponse, err := TemplateResponse(ctx, d, &tmpl, out.Repository.Author)
		if err != nil {
			return nil, err
		}

		out.Templates = append(out.Templates, tmplResponse)
	}
	return out, nil
}

func TemplateResponse(ctx context.Context, d dependencies.ForProjectRequest, tmpl *repository.TemplateRecord, author *Author) (out *Template, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.TemplateResponse")
	defer telemetry.EndSpan(span, &err)

	defaultVersion, err := tmpl.DefaultVersionOrErr()
	if err != nil {
		return nil, err
	}

	out = &Template{
		ID:             tmpl.ID,
		Name:           tmpl.Name,
		Components:     ComponentsResponse(d, defaultVersion.Components),
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         author,
		Versions:       make([]*Version, 0),
	}

	for _, version := range tmpl.Versions {
		version := version
		out.Versions = append(out.Versions, VersionResponse(&version))
	}
	return out, nil
}

func TemplateDetailResponse(ctx context.Context, d dependencies.ForProjectRequest, repo *repository.Repository, tmpl *repository.TemplateRecord) (out *TemplateDetail, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.TemplateDetailResponse")
	defer telemetry.EndSpan(span, &err)

	defaultVersion, err := tmpl.DefaultVersionOrErr()
	if err != nil {
		return nil, err
	}

	repoResponse := RepositoryResponse(ctx, d, repo)
	out = &TemplateDetail{
		Repository:     repoResponse,
		ID:             tmpl.ID,
		Name:           tmpl.Name,
		Components:     ComponentsResponse(d, defaultVersion.Components),
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         repoResponse.Author,
		Versions:       make([]*Version, 0),
	}
	for _, version := range tmpl.Versions {
		version := version
		out.Versions = append(out.Versions, VersionResponse(&version))
	}
	return out, nil
}

func VersionResponse(v *repository.VersionRecord) *Version {
	return &Version{
		Version:     v.Version.String(),
		Stable:      v.Stable,
		Description: v.Description,
	}
}

func VersionDetailResponse(d dependencies.ForProjectRequest, template *template.Template) *VersionDetail {
	versionRec := template.VersionRecord()
	return &VersionDetail{
		Version:         versionRec.Version.String(),
		Stable:          versionRec.Stable,
		Description:     versionRec.Description,
		Components:      ComponentsResponse(d, template.Components()),
		LongDescription: template.LongDesc(),
		Readme:          template.Readme(),
	}
}

func VersionDetailExtendedResponse(ctx context.Context, d dependencies.ForProjectRequest, repo *repository.Repository, template *template.Template) (out *VersionDetailExtended, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.VersionDetailExtendedResponse")
	defer telemetry.EndSpan(span, &err)

	repoResponse := RepositoryResponse(ctx, d, repo)
	tmplRec := template.TemplateRecord()
	versionRec := template.VersionRecord()
	tmplResponse, err := TemplateResponse(ctx, d, &tmplRec, repoResponse.Author)
	if err != nil {
		return nil, err
	}

	return &VersionDetailExtended{
		Repository:      repoResponse,
		Template:        tmplResponse,
		Version:         versionRec.Version.String(),
		Stable:          versionRec.Stable,
		Description:     versionRec.Description,
		Components:      ComponentsResponse(d, template.Components()),
		LongDescription: template.LongDesc(),
		Readme:          template.Readme(),
	}, nil
}

// ComponentsResponse replaces placeholder in components list.
// The original order is preserved, it is used in the UI.
func ComponentsResponse(d dependencies.ForProjectRequest, in []string) (out []string) {
	out = make([]string, 0)
	for _, componentId := range in {
		// Map placeholder "<keboola.wr-snowflake>" to real componentId.
		if componentId == manifest.SnowflakeWriterComponentIDPlaceholder {
			if _, found := d.Components().Get(function.SnowflakeWriterIDAws); found {
				componentId = function.SnowflakeWriterIDAws.String()
			} else if _, found := d.Components().Get(function.SnowflakeWriterIDAzure); found {
				componentId = function.SnowflakeWriterIDAzure.String()
			} else {
				continue
			}
		}
		out = append(out, componentId)
	}
	return out
}

func UpgradeInstanceInputsResponse(ctx context.Context, d dependencies.ForProjectRequest, prjState *project.State, branchKey model.BranchKey, instance *model.TemplateInstance, tmpl *template.Template) (out *Inputs) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.UpgradeInstanceInputsResponse")
	defer telemetry.EndSpan(span, nil)

	stepsGroupsExt := upgrade.ExportInputsValues(d.Logger().InfoWriter(), prjState.State(), branchKey, instance.InstanceID, tmpl.Inputs())
	return InputsResponse(ctx, d, stepsGroupsExt)
}

func InputsResponse(ctx context.Context, d dependencies.ForProjectRequest, stepsGroups input.StepsGroupsExt) (out *Inputs) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.InputsResponse")
	defer telemetry.EndSpan(span, nil)

	out = &Inputs{StepGroups: make([]*StepGroup, 0)}
	initialValues := make([]*StepPayload, 0)

	// Groups
	for _, group := range stepsGroups {
		// Group
		groupResponse := &StepGroup{
			ID:          group.ID,
			Description: group.Description,
			Required:    string(group.Required),
			Steps:       make([]*Step, 0),
		}
		out.StepGroups = append(out.StepGroups, groupResponse)

		// Steps
		for _, step := range group.Steps {
			// If the step is pre-configured -> validate default values.
			var stepValues *StepPayload
			if step.Show {
				stepValues = &StepPayload{ID: step.ID}
				initialValues = append(initialValues, stepValues)
			}

			// Step
			stepResponse := &Step{
				ID:                step.ID,
				Icon:              step.Icon,
				Name:              step.Name,
				Description:       step.Description,
				DialogName:        step.NameForDialog(),
				DialogDescription: step.DescriptionForDialog(),
				Inputs:            make([]*Input, 0),
			}
			groupResponse.Steps = append(groupResponse.Steps, stepResponse)

			// Inputs
			for _, in := range step.Inputs {
				inputResponse := &Input{
					ID:          in.ID,
					Name:        in.Name,
					Description: in.Description,
					Type:        string(in.Type),
					Kind:        string(in.Kind),
					Default:     in.DefaultOrEmpty(),
					Options:     OptionsResponse(in.Options),
				}
				if in.ComponentID != "" {
					v := in.ComponentID
					inputResponse.ComponentID = &v
				}
				if in.OauthInputID != "" {
					v := in.OauthInputID
					inputResponse.OauthInputID = &v
				}
				stepResponse.Inputs = append(stepResponse.Inputs, inputResponse)

				if stepValues != nil {
					stepValues.Inputs = append(stepValues.Inputs, &InputValue{ID: inputResponse.ID, Value: inputResponse.Default})
				}
			}
		}
	}

	// Together with the inputs definitions, the initial state (initial validation) is generated.
	// It is primarily intended for the upgrade operation, where the step may be pre-configured.
	out.InitialState, _, _ = validateInputs(stepsGroups.ToValue(), initialValues)
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

func InstancesResponse(ctx context.Context, d dependencies.ForProjectRequest, prjState *project.State, branchKey model.BranchKey) (out *Instances, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.InstancesResponse")
	defer telemetry.EndSpan(span, &err)

	// Get branch state
	branch, found := prjState.GetOrNil(branchKey).(*model.BranchState)
	if !found {
		return nil, &GenericError{
			Name:    "templates.branchNotFound",
			Message: fmt.Sprintf(`Branch "%d" not found.`, branchKey.ID),
		}
	}

	// Get instances
	instances, err := branch.Remote.Metadata.TemplatesInstances()
	if err != nil {
		return nil, err
	}

	// Map response
	out = &Instances{Instances: make([]*Instance, 0)}
	for _, instance := range instances {
		// Skip instance if the repository is no more defined for the project
		if _, found := d.ProjectRepositories().Get(instance.RepositoryName); !found {
			continue
		}

		outInstance := &Instance{
			TemplateID:     instance.TemplateID,
			InstanceID:     instance.InstanceID,
			Branch:         cast.ToString(branch.ID),
			RepositoryName: instance.RepositoryName,
			Version:        instance.Version,
			Name:           instance.InstanceName,
			Created: &ChangeInfo{
				Date:    instance.Created.Date.Format(time.RFC3339),
				TokenID: instance.Created.TokenID,
			},
			Updated: &ChangeInfo{
				Date:    instance.Updated.Date.Format(time.RFC3339),
				TokenID: instance.Updated.TokenID,
			},
		}

		if instance.MainConfig != nil {
			configKey := model.ConfigKey{BranchID: branchKey.ID, ComponentID: instance.MainConfig.ComponentID, ID: instance.MainConfig.ConfigID}
			if _, found := prjState.Get(configKey); found {
				outInstance.MainConfig = &MainConfig{
					ComponentID: string(instance.MainConfig.ComponentID),
					ConfigID:    string(instance.MainConfig.ConfigID),
				}
			}
		}

		out.Instances = append(out.Instances, outInstance)
	}

	return out, nil
}

func InstanceResponse(ctx context.Context, d dependencies.ForProjectRequest, prjState *project.State, branchKey model.BranchKey, instanceId string) (out *InstanceDetail, err error) {
	ctx, span := d.Tracer().Start(ctx, "api.server.templates.mapper.InstanceResponse")
	defer telemetry.EndSpan(span, &err)

	// Get branch state
	branch, found := prjState.GetOrNil(branchKey).(*model.BranchState)
	if !found {
		return nil, &GenericError{
			Name:    "templates.branchNotFound",
			Message: fmt.Sprintf(`Branch "%d" not found.`, branchKey.ID),
		}
	}

	// Get instances
	instance, found, err := branch.LocalOrRemoteState().(*model.Branch).Metadata.TemplateInstance(instanceId)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, &GenericError{
			Name:    "templates.instanceNotFound",
			Message: fmt.Sprintf(`Instance "%s" not found in branch "%d".`, instanceId, branchKey.ID),
		}
	}

	// Check if the repository is still defined in the project
	if _, found := d.ProjectRepositories().Get(instance.RepositoryName); !found {
		return nil, &GenericError{
			Name:    "templates.repositoryNotFound",
			Message: fmt.Sprintf(`Repository "%s" not found.`, instance.RepositoryName),
		}
	}

	// Map configurations
	outConfigs := make([]*Config, 0)
	branchConfigs := prjState.RemoteObjects().ConfigsWithRowsFrom(branchKey)
	for _, config := range search.ConfigsForTemplateInstance(branchConfigs, instanceId) {
		outConfigs = append(outConfigs, &Config{
			Name:        config.Name,
			ConfigID:    string(config.ID),
			ComponentID: string(config.ComponentID),
		})
	}

	// Map response
	out = &InstanceDetail{
		VersionDetail:  instanceVersionDetail(ctx, d, instance),
		TemplateID:     instance.TemplateID,
		InstanceID:     instance.InstanceID,
		Branch:         cast.ToString(branch.ID),
		RepositoryName: instance.RepositoryName,
		Version:        instance.Version,
		Name:           instance.InstanceName,
		Created: &ChangeInfo{
			Date:    instance.Created.Date.Format(time.RFC3339),
			TokenID: instance.Created.TokenID,
		},
		Updated: &ChangeInfo{
			Date:    instance.Updated.Date.Format(time.RFC3339),
			TokenID: instance.Updated.TokenID,
		},
		Configurations: outConfigs,
	}

	// Main config
	if instance.MainConfig != nil {
		configKey := model.ConfigKey{BranchID: branchKey.ID, ComponentID: instance.MainConfig.ComponentID, ID: instance.MainConfig.ConfigID}
		if _, found := prjState.Get(configKey); found {
			out.MainConfig = &MainConfig{
				ComponentID: string(instance.MainConfig.ComponentID),
				ConfigID:    string(instance.MainConfig.ConfigID),
			}
		}
	}

	return out, nil
}

func instanceVersionDetail(ctx context.Context, d dependencies.ForProjectRequest, instance *model.TemplateInstance) *VersionDetail {
	repo, tmplRecord, err := templateRecord(d, instance.RepositoryName, instance.TemplateID)
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
	tmpl, err := d.Template(ctx, model.NewTemplateRef(repo.Definition(), instance.TemplateID, versionRecord.Version.String()))
	if err != nil {
		return nil
	}
	return VersionDetailResponse(d, tmpl)
}
