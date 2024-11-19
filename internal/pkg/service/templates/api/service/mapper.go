package service

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/spf13/cast"

	templatesDesign "github.com/keboola/keboola-as-code/api/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	KeboolaDataApps   = "keboola.data-apps"
	KeboolaComponents = "keboola.components"
)

type Mapper struct {
	apiHost string
}

type mapperDependencies interface {
	APIConfig() config.Config
}

func NewMapper(d mapperDependencies) *Mapper {
	return &Mapper{
		apiHost: d.APIConfig().API.PublicURL.String(),
	}
}

func (m Mapper) TaskPayload(model task.Task) (r *Task) {
	out := &Task{
		ID:        model.TaskID,
		Type:      model.Type,
		URL:       formatTaskURL(m.apiHost, model.Key),
		CreatedAt: model.CreatedAt.String(),
	}

	if model.FinishedAt != nil {
		v := model.FinishedAt.String()
		out.FinishedAt = &v
	}

	if model.Duration != nil {
		v := model.Duration.Milliseconds()
		out.Duration = &v
	}

	switch {
	case model.IsProcessing():
		out.Status = templatesDesign.TaskStatusProcessing
	case model.IsSuccessful():
		out.Status = templatesDesign.TaskStatusSuccess
		out.IsFinished = true
		out.Result = &model.Result
	case model.IsFailed():
		out.Status = templatesDesign.TaskStatusError
		out.IsFinished = true
		out.Error = &model.Error
	default:
		panic(errors.New("unexpected task status"))
	}

	if model.Outputs != nil {
		if v, ok := model.Outputs["instanceId"].(string); ok {
			out.Outputs = &TaskOutputs{
				InstanceID: &v,
			}
		}
	}

	return out
}

func formatTaskURL(apiHost string, k task.Key) string {
	return fmt.Sprintf("%s/v1/tasks/%s", apiHost, k.TaskID)
}

func RepositoriesResponse(ctx context.Context, d dependencies.ProjectRequestScope) (out *Repositories, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.RepositoriesResponse")
	defer span.End(&err)

	out = &Repositories{}
	for _, repoRef := range d.ProjectRepositories().All() {
		repo, err := repositoryInst(ctx, d, repoRef.Name)
		if err != nil {
			return nil, err
		}
		out.Repositories = append(out.Repositories, RepositoryResponse(ctx, d, repo))
	}
	return out, nil
}

func RepositoryResponse(ctx context.Context, d dependencies.ProjectRequestScope, v *repository.Repository) *Repository {
	_, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.RepositoryResponse")
	defer span.End(nil)

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

func TemplatesResponse(ctx context.Context, d dependencies.ProjectRequestScope, repo *repository.Repository, templates []repository.TemplateRecord, filterBy *string) (out *Templates, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.TemplatesResponse")
	defer span.End(&err)

	out = &Templates{Repository: RepositoryResponse(ctx, d, repo), Templates: make([]*Template, 0)}
	for _, tmpl := range templates {
		// Exclude deprecated templates from the list
		if tmpl.Deprecated {
			continue
		}

		if filterBy != nil && *filterBy != "" {
			t, found := tmpl.DefaultVersion()
			if !found {
				continue
			}

			filterString := *filterBy
			switch filterString {
			case KeboolaDataApps:
				if !slices.Contains(t.Components, filterString) {
					continue
				}
			case KeboolaComponents:
				if slices.Contains(t.Components, KeboolaDataApps) {
					continue
				}
			}
		}

		if !hasRequirements(tmpl, d) {
			continue
		}
		tmplResponse, err := TemplateResponse(ctx, d, &tmpl, out.Repository.Author)
		if err != nil {
			return nil, err
		}

		out.Templates = append(out.Templates, tmplResponse)
	}
	return out, nil
}

func TemplateResponse(ctx context.Context, d dependencies.ProjectRequestScope, tmpl *repository.TemplateRecord, author *Author) (out *Template, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.TemplateResponse")
	defer span.End(&err)

	defaultVersion, err := tmpl.DefaultVersionOrErr()
	if err != nil {
		return nil, err
	}

	out = &Template{
		ID:             tmpl.ID,
		Name:           tmpl.Name,
		Deprecated:     tmpl.Deprecated,
		Categories:     CategoriesResponse(tmpl.Categories),
		Components:     ComponentsResponse(d, defaultVersion.Components),
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         author,
		Versions:       make([]*Version, 0),
	}

	for _, version := range tmpl.Versions {
		out.Versions = append(out.Versions, VersionResponse(&version))
	}
	return out, nil
}

func TemplateDetailResponse(ctx context.Context, d dependencies.ProjectRequestScope, repo *repository.Repository, tmpl *repository.TemplateRecord) (out *TemplateDetail, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.TemplateDetailResponse")
	defer span.End(&err)

	defaultVersion, err := tmpl.DefaultVersionOrErr()
	if err != nil {
		return nil, err
	}

	repoResponse := RepositoryResponse(ctx, d, repo)
	out = &TemplateDetail{
		Repository:     repoResponse,
		ID:             tmpl.ID,
		Name:           tmpl.Name,
		Deprecated:     tmpl.Deprecated,
		Categories:     CategoriesResponse(tmpl.Categories),
		Components:     ComponentsResponse(d, defaultVersion.Components),
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         repoResponse.Author,
		Versions:       make([]*Version, 0),
	}
	for _, version := range tmpl.Versions {
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

func VersionDetailResponse(d dependencies.ProjectRequestScope, version repository.VersionRecord, template *template.Template) *VersionDetail {
	var longDescription string
	var readme string
	if template != nil {
		longDescription = template.LongDesc()
		readme = template.Readme()
	}

	return &VersionDetail{
		Version:         version.Version.String(),
		Stable:          version.Stable,
		Description:     version.Description,
		Components:      ComponentsResponse(d, version.Components),
		LongDescription: longDescription,
		Readme:          readme,
	}
}

func VersionDetailExtendedResponse(ctx context.Context, d dependencies.ProjectRequestScope, repo *repository.Repository, template *template.Template) (out *VersionDetailExtended, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.VersionDetailExtendedResponse")
	defer span.End(&err)

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

// CategoriesResponse returns default "Other" category if the list is empty.
func CategoriesResponse(in []string) []string {
	if len(in) == 0 {
		return []string{"Other"}
	}
	return in
}

// ComponentsResponse replaces placeholder in components list.
// The original order is preserved, it is used in the UI.
func ComponentsResponse(d dependencies.ProjectRequestScope, in []string) (out []string) {
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

func UpgradeInstanceInputsResponse(ctx context.Context, d dependencies.ProjectRequestScope, prjState *project.State, branchKey model.BranchKey, instance *model.TemplateInstance, tmpl *template.Template) (out *Inputs) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.UpgradeInstanceInputsResponse")
	defer span.End(nil)

	stepsGroupsExt := upgrade.ExportInputsValues(ctx, d.Logger().Infof, prjState.State(), branchKey, instance.InstanceID, tmpl.Inputs())
	return InputsResponse(ctx, d, stepsGroupsExt)
}

func InputsResponse(ctx context.Context, d dependencies.ProjectRequestScope, stepsGroups input.StepsGroupsExt) (out *Inputs) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.InputsResponse")
	defer span.End(nil)

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
	out.InitialState, _, _ = validateInputs(ctx, stepsGroups.ToValue(), initialValues)
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

func InstancesResponse(ctx context.Context, d dependencies.ProjectRequestScope, prjState *project.State, branchKey model.BranchKey) (out *Instances, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.InstancesResponse")
	defer span.End(&err)

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

	// Group configurations by instance
	configs := configurationsByInstance(prjState, branchKey)

	// Map response
	out = &Instances{Instances: make([]*Instance, 0)}
	for _, instance := range instances {
		// Skip instance if the repository is no more defined for the project
		if _, found := d.ProjectRepositories().Get(instance.RepositoryName); !found {
			continue
		}

		outInstance := &Instance{
			InstanceID:     instance.InstanceID,
			TemplateID:     instance.TemplateID,
			RepositoryName: instance.RepositoryName,
			Branch:         cast.ToString(branch.ID),
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
			Configurations: configs[instance.InstanceID],
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

func InstanceResponse(ctx context.Context, d dependencies.ProjectRequestScope, prjState *project.State, branchKey model.BranchKey, instanceId string) (out *InstanceDetail, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.InstanceResponse")
	defer span.End(&err)

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

	// Map response
	tmplResponse, versionResponse := instanceDetails(ctx, d, instance)
	out = &InstanceDetail{
		InstanceID:     instance.InstanceID,
		TemplateID:     instance.TemplateID,
		Version:        instance.Version,
		RepositoryName: instance.RepositoryName,
		Branch:         cast.ToString(branch.ID),
		Name:           instance.InstanceName,
		Created: &ChangeInfo{
			Date:    instance.Created.Date.Format(time.RFC3339),
			TokenID: instance.Created.TokenID,
		},
		Updated: &ChangeInfo{
			Date:    instance.Updated.Date.Format(time.RFC3339),
			TokenID: instance.Updated.TokenID,
		},
		TemplateDetail: tmplResponse,
		VersionDetail:  versionResponse,
		Configurations: instanceConfigurations(prjState, branchKey, instanceId),
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

func instanceConfigurations(prjState *project.State, branchKey model.BranchKey, instanceId string) []*Config {
	out := make([]*Config, 0)
	branchConfigs := prjState.RemoteObjects().ConfigsWithRowsFrom(branchKey)
	for _, cfg := range search.ConfigsForTemplateInstance(branchConfigs, instanceId) {
		out = append(out, &Config{
			Name:        cfg.Name,
			ConfigID:    string(cfg.ID),
			ComponentID: string(cfg.ComponentID),
		})
	}
	return out
}

func configurationsByInstance(prjState *project.State, branchKey model.BranchKey) map[string][]*Config {
	out := make(map[string][]*Config, 0)
	branchConfigs := prjState.RemoteObjects().ConfigsWithRowsFrom(branchKey)
	for instanceID, configs := range search.ConfigsByTemplateInstance(branchConfigs) {
		for _, cfg := range configs {
			out[instanceID] = append(out[instanceID], &Config{
				Name:        cfg.Name,
				ConfigID:    string(cfg.ID),
				ComponentID: string(cfg.ComponentID),
			})
		}
		results := out[instanceID]
		sort.SliceStable(results, func(i, j int) bool {
			return results[i].Name < results[j].Name
		})
	}
	return out
}

func templateBaseResponse(ctx context.Context, d dependencies.ProjectRequestScope, tmpl *repository.TemplateRecord, author *Author) (out *TemplateBase, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.mapper.templateBaseResponse")
	defer span.End(&err)

	defaultVersion, err := tmpl.DefaultVersionOrErr()
	if err != nil {
		return nil, err
	}

	return &TemplateBase{
		ID:             tmpl.ID,
		Name:           tmpl.Name,
		Deprecated:     tmpl.Deprecated,
		Description:    tmpl.Description,
		DefaultVersion: defaultVersion.Version.String(),
		Author:         author,
	}, nil
}

func instanceDetails(ctx context.Context, d dependencies.ProjectRequestScope, instance *model.TemplateInstance) (*TemplateBase, *VersionDetail) {
	repo, tmplRecord, err := templateRecord(ctx, d, instance.RepositoryName, instance.TemplateID)
	if err != nil {
		return nil, nil
	}
	semVer, err := model.NewSemVersion(instance.Version)
	if err != nil {
		return nil, nil
	}
	versionRecord, found := tmplRecord.GetClosestVersion(semVer)
	if !found {
		return nil, nil
	}

	// Tmpl may be nil, if the template is deprecated, it cannot be loaded.
	tmpl, _ := d.Template(ctx, model.NewTemplateRef(repo.Definition(), instance.TemplateID, versionRecord.Version.String()))

	// Template info
	author := repo.Manifest().Author()
	tmplResponse, err := templateBaseResponse(ctx, d, tmplRecord, &Author{Name: author.Name, URL: author.URL})
	if err != nil {
		return nil, nil
	}

	// Version info
	versionResponse := VersionDetailResponse(d, versionRecord, tmpl)

	return tmplResponse, versionResponse
}

func hasRequirements(tmpl repository.TemplateRecord, d dependencies.ProjectRequestScope) bool {
	if !tmpl.HasBackend(d.ProjectBackends()) {
		return false
	}

	if !tmpl.CheckProjectComponents(d.Components()) {
		return false
	}

	if !tmpl.CheckProjectFeatures(d.ProjectFeatures()) {
		return false
	}
	return true
}
