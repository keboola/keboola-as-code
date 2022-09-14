package service

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	deleteTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
	renameInst "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const ProjectLockedRetryAfter = 5 * time.Second

type service struct{}

func New(d dependencies.ForServer) (Service, error) {
	if err := StartComponentsCron(d.ServerCtx(), d); err != nil {
		return nil, err
	}

	if err := StartRepositoriesPullCron(d.ServerCtx(), d); err != nil {
		return nil, err
	}

	return &service{}, nil
}

func (s *service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(dependencies.ForPublicRequest) (res *ServiceDetail, err error) {
	res = &ServiceDetail{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(dependencies.ForPublicRequest) (res string, err error) {
	return "OK", nil
}

func (s *service) RepositoriesIndex(d dependencies.ForProjectRequest, _ *RepositoriesIndexPayload) (res *Repositories, err error) {
	return RepositoriesResponse(d.RequestCtx(), d, d.ProjectRepositories())
}

func (s *service) RepositoryIndex(d dependencies.ForProjectRequest, payload *RepositoryIndexPayload) (res *Repository, err error) {
	repo, err := repositoryInst(d, payload.Repository)
	if err != nil {
		return nil, err
	}
	return RepositoryResponse(d.RequestCtx(), d, repo), nil
}

func (s *service) TemplatesIndex(d dependencies.ForProjectRequest, payload *TemplatesIndexPayload) (res *Templates, err error) {
	repo, err := repositoryInst(d, payload.Repository)
	if err != nil {
		return nil, err
	}
	return TemplatesResponse(d.RequestCtx(), d, repo, repo.Templates())
}

func (s *service) TemplateIndex(d dependencies.ForProjectRequest, payload *TemplateIndexPayload) (res *TemplateDetail, err error) {
	repo, tmplRecord, err := templateRecord(d, payload.Repository, payload.Template)
	if err != nil {
		return nil, err
	}
	return TemplateDetailResponse(d.RequestCtx(), d, repo, tmplRecord)
}

func (s *service) VersionIndex(d dependencies.ForProjectRequest, payload *VersionIndexPayload) (res *VersionDetailExtended, err error) {
	repo, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return VersionDetailExtendedResponse(d.RequestCtx(), d, repo, tmpl)
}

func (s *service) InputsIndex(d dependencies.ForProjectRequest, payload *InputsIndexPayload) (res *Inputs, err error) {
	_, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return InputsResponse(d.RequestCtx(), d, tmpl.Inputs().ToExtended()), nil
}

func (s *service) ValidateInputs(d dependencies.ForProjectRequest, payload *ValidateInputsPayload) (res *ValidationResult, err error) {
	_, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, _, err := validateInputs(tmpl.Inputs(), payload.Steps)
	return result, err
}

func (s *service) UseTemplateVersion(d dependencies.ForProjectRequest, payload *UseTemplateVersionPayload) (res *UseTemplateResult, err error) {
	// Lock project
	if unlockFn, err := tryLockProject(d); err != nil {
		return nil, err
	} else {
		defer unlockFn()
	}

	// Note:
	//   A very strange code follows.
	//   Since I did not manage to complete the refactoring - separation of remote and local state.
	//   A virtual FS and fake manifest is created to make it work.

	branchKey, err := getBranch(d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, values, err := validateInputs(tmpl.Inputs(), payload.Steps)
	if err != nil {
		return nil, err
	}
	if !result.Valid {
		return nil, &ValidationError{
			Name:             "InvalidInputs",
			Message:          "Inputs are not valid.",
			ValidationResult: result,
		}
	}

	// Create virtual fs, after refactoring it will be removed
	fs, err := aferofs.NewMemoryFs(d.Logger(), "")
	if err != nil {
		return nil, err
	}

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Load only target branch
	m.Filter().SetAllowedKeys([]model.Key{branchKey})
	prj := project.NewWithManifest(d.RequestCtx(), fs, m)

	// Load project state
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
	if err != nil {
		return nil, err
	}

	// Copy remote state to the local
	for _, objectState := range prjState.All() {
		objectState.SetLocalState(deepcopy.Copy(objectState.RemoteState()).(model.Object))
	}

	// Options
	options := useTemplate.Options{
		InstanceName: payload.Name,
		TargetBranch: branchKey,
		Inputs:       values,
	}

	// Use template
	instanceId, _, err := useTemplate.Run(d.RequestCtx(), prjState, tmpl, options, d)
	if err != nil {
		return nil, err
	}

	// Push changes
	changeDesc := fmt.Sprintf("From template %s", tmpl.FullName())
	if err := push.Run(d.RequestCtx(), prjState, push.Options{ChangeDescription: changeDesc, SkipValidation: true}, d); err != nil {
		return nil, err
	}

	return &UseTemplateResult{InstanceID: instanceId}, nil
}

func (s *service) InstancesIndex(d dependencies.ForProjectRequest, payload *InstancesIndexPayload) (res *Instances, err error) {
	branchKey, err := getBranch(d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Create virtual fs, after refactoring it will be removed
	fs, err := aferofs.NewMemoryFs(d.Logger(), "")
	if err != nil {
		return nil, err
	}

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Only one branch
	m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.Id))})
	prj := project.NewWithManifest(d.RequestCtx(), fs, m)

	// Load project state
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
	if err != nil {
		return nil, err
	}

	return InstancesResponse(d.RequestCtx(), d, prjState, branchKey)
}

func (s *service) InstanceIndex(d dependencies.ForProjectRequest, payload *InstanceIndexPayload) (res *InstanceDetail, err error) {
	_, span := d.Tracer().Start(d.RequestCtx(), "api.server.templates.service.InstanceIndex")
	defer telemetry.EndSpan(span, &err)

	branchKey, err := getBranch(d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Create virtual fs, after refactoring it will be removed
	fs, err := aferofs.NewMemoryFs(d.Logger(), "")
	if err != nil {
		return nil, err
	}

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Only one branch
	m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.Id))})
	prj := project.NewWithManifest(d.RequestCtx(), fs, m)

	// Load project state
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
	if err != nil {
		return nil, err
	}
	return InstanceResponse(d.RequestCtx(), d, prjState, branchKey, payload.InstanceID)
}

func (s *service) UpdateInstance(d dependencies.ForProjectRequest, payload *UpdateInstancePayload) (res *InstanceDetail, err error) {
	// Lock project
	if unlockFn, err := tryLockProject(d); err != nil {
		return nil, err
	} else {
		defer unlockFn()
	}

	// Get instance
	prjState, branchKey, instance, err := getTemplateInstance(d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	opts := renameInst.Options{
		Branch:   branchKey,
		Instance: *instance,
		NewName:  payload.Name,
	}

	err = renameInst.Run(d.RequestCtx(), prjState, opts, d)
	if err != nil {
		return nil, err
	}

	// Push changes
	changeDesc := fmt.Sprintf("Rename template instance %s", payload.InstanceID)
	if err := push.Run(d.RequestCtx(), prjState, push.Options{ChangeDescription: changeDesc, AllowRemoteDelete: true, DryRun: false, SkipValidation: true}, d); err != nil {
		return nil, err
	}

	return InstanceResponse(d.RequestCtx(), d, prjState, branchKey, payload.InstanceID)
}

func (s *service) DeleteInstance(d dependencies.ForProjectRequest, payload *DeleteInstancePayload) error {
	// Lock project
	if unlockFn, err := tryLockProject(d); err != nil {
		return err
	} else {
		defer unlockFn()
	}

	// Get instance
	prjState, branchKey, _, err := getTemplateInstance(d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return err
	}

	// Delete template instance
	deleteOpts := deleteTemplate.Options{
		Branch:   branchKey,
		DryRun:   false,
		Instance: payload.InstanceID,
	}
	err = deleteTemplate.Run(d.RequestCtx(), prjState, deleteOpts, d)
	if err != nil {
		return err
	}

	// Push changes
	changeDesc := fmt.Sprintf("Delete template instance %s", payload.InstanceID)
	if err := push.Run(d.RequestCtx(), prjState, push.Options{ChangeDescription: changeDesc, AllowRemoteDelete: true, DryRun: false, SkipValidation: true}, d); err != nil {
		return err
	}

	return nil
}

func (s *service) UpgradeInstance(d dependencies.ForProjectRequest, payload *UpgradeInstancePayload) (res *UpgradeInstanceResult, err error) {
	// Lock project
	if unlockFn, err := tryLockProject(d); err != nil {
		return nil, err
	} else {
		defer unlockFn()
	}

	// Get instance
	prjState, branchKey, instance, err := getTemplateInstance(d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(d, instance.RepositoryName, instance.TemplateId, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, values, err := validateInputs(tmpl.Inputs(), payload.Steps)
	if err != nil {
		return nil, err
	}
	if !result.Valid {
		return nil, &ValidationError{
			Name:             "InvalidInputs",
			Message:          "Inputs are not valid.",
			ValidationResult: result,
		}
	}

	// Upgrade template instance
	upgradeOpts := upgradeTemplate.Options{
		Branch:   branchKey,
		Instance: *instance,
		Inputs:   values,
	}
	_, err = upgradeTemplate.Run(d.RequestCtx(), prjState, tmpl, upgradeOpts, d)
	if err != nil {
		return nil, err
	}

	// Push changes
	changeDesc := fmt.Sprintf("Upgraded from template %s", tmpl.FullName())
	if err := push.Run(d.RequestCtx(), prjState, push.Options{ChangeDescription: changeDesc, AllowRemoteDelete: true, DryRun: false, SkipValidation: true}, d); err != nil {
		return nil, err
	}

	return &UpgradeInstanceResult{InstanceID: instance.InstanceId}, nil
}

func (s *service) UpgradeInstanceInputsIndex(d dependencies.ForProjectRequest, payload *UpgradeInstanceInputsIndexPayload) (res *Inputs, err error) {
	// Get instance
	prjState, branchKey, instance, err := getTemplateInstance(d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(d, instance.RepositoryName, instance.TemplateId, payload.Version)
	if err != nil {
		return nil, err
	}

	// Generate response
	return UpgradeInstanceInputsResponse(d.RequestCtx(), d, prjState, branchKey, instance, tmpl), nil
}

func (s *service) UpgradeInstanceValidateInputs(d dependencies.ForProjectRequest, payload *UpgradeInstanceValidateInputsPayload) (res *ValidationResult, err error) {
	// Get instance
	_, _, instance, err := getTemplateInstance(d, payload.Branch, payload.InstanceID, false)
	if err != nil {
		return nil, err
	}

	// Validate the inputs as in the use operation
	return s.ValidateInputs(d, &ValidateInputsPayload{
		Repository:      instance.RepositoryName,
		Template:        instance.TemplateId,
		Version:         payload.Version,
		Steps:           payload.Steps,
		StorageAPIToken: payload.StorageAPIToken,
	})
}

func repositoryRef(d dependencies.ForProjectRequest, name string) (model.TemplateRepository, error) {
	for _, repo := range d.ProjectRepositories() {
		if repo.Name == name {
			return repo, nil
		}
	}
	return model.TemplateRepository{}, &GenericError{
		Name:    "templates.repositoryNotFound",
		Message: fmt.Sprintf(`Repository "%s" not found.`, name),
	}
}

func repositoryInst(d dependencies.ForProjectRequest, repoName string) (*repository.Repository, error) {
	// Get repository ref
	repoRef, err := repositoryRef(d, repoName)
	if err != nil {
		return nil, err
	}

	// Get repository
	repo, err := d.TemplateRepository(d.RequestCtx(), repoRef)
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func templateRecord(d dependencies.ForProjectRequest, repoName, templateId string) (*repository.Repository, *repository.TemplateRecord, error) {
	// Get repository
	repo, err := repositoryInst(d, repoName)
	if err != nil {
		return nil, nil, err
	}

	// Get template record
	tmpl, found := repo.RecordById(templateId)
	if !found {
		return nil, nil, &GenericError{
			Name:    "templates.templateNotFound",
			Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
		}
	}
	return repo, &tmpl, nil
}

func getTemplateVersion(d dependencies.ForProjectRequest, repoName, templateId, versionStr string) (*repository.Repository, *template.Template, error) {
	// Get repo
	repo, err := repositoryInst(d, repoName)
	if err != nil {
		return nil, nil, err
	}

	// Parse version
	var semVersion model.SemVersion
	if versionStr == "default" {
		// Default version
		tmplRecord, found := repo.RecordById(templateId)
		if !found {
			return nil, nil, &GenericError{
				Name:    "templates.templateNotFound",
				Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
			}
		}
		if versionRecord, err := tmplRecord.DefaultVersionOrErr(); err != nil {
			return nil, nil, &GenericError{
				Name:    "templates.templateNotFound",
				Message: err.Error(),
			}
		} else {
			semVersion = versionRecord.Version
		}
	} else if v, err := model.NewSemVersion(versionStr); err != nil {
		// Invalid semantic version
		return nil, nil, &BadRequestError{
			Message: fmt.Sprintf(`Version "%s" is not valid: %s`, versionStr, err),
		}
	} else {
		// Parsed version
		semVersion = v
	}

	// Get template version
	tmpl, err := d.Template(d.RequestCtx(), model.NewTemplateRef(repo.Definition(), templateId, semVersion.Original()))
	if err != nil {
		if errors.As(err, &manifest.TemplateNotFoundError{}) {
			return nil, nil, &GenericError{
				Name:    "templates.templateNotFound",
				Message: fmt.Sprintf(`Template "%s" not found.`, templateId),
			}
		}
		if errors.As(err, &manifest.VersionNotFoundError{}) {
			return nil, nil, &GenericError{
				Name:    "templates.versionNotFound",
				Message: fmt.Sprintf(`Version "%s" not found.`, versionStr),
			}
		}
		return nil, nil, err
	}

	return repo, tmpl, nil
}

func getBranch(d dependencies.ForProjectRequest, branchDef string) (model.BranchKey, error) {
	// Get Storage API
	storageApiClient := d.StorageApiClient()

	// Parse branch ID
	var targetBranch model.BranchKey
	if branchDef == "default" {
		// Use main branch
		if v, err := storageapi.GetDefaultBranchRequest().Send(d.RequestCtx(), storageApiClient); err != nil {
			return targetBranch, err
		} else {
			targetBranch.Id = v.ID
		}
	} else if branchId, err := strconv.Atoi(branchDef); err != nil {
		// Branch ID must be numeric
		return targetBranch, BadRequestError{
			Message: fmt.Sprintf(`branch ID "%s" is not numeric`, branchDef),
		}
	} else if _, err := storageapi.GetBranchRequest(storageapi.BranchKey{ID: storageapi.BranchID(branchId)}).Send(d.RequestCtx(), storageApiClient); err != nil {
		// Branch not found
		return targetBranch, &GenericError{
			Name:    "templates.branchNotFound",
			Message: fmt.Sprintf(`Branch "%d" not found.`, branchId),
		}
	} else {
		// Branch found
		targetBranch.Id = storageapi.BranchID(branchId)
	}

	return targetBranch, nil
}

func getTemplateInstance(d dependencies.ForProjectRequest, branchDef, instanceId string, loadConfigs bool) (*project.State, model.BranchKey, *model.TemplateInstance, error) {
	// Note:
	//   Waits for separation of remote and local state.
	//   A virtual FS and fake manifest are created to make it work.

	branchKey, err := getBranch(d, branchDef)
	if err != nil {
		return nil, branchKey, nil, err
	}

	// Create virtual fs, after refactoring it will be removed
	fs, err := aferofs.NewMemoryFs(d.Logger(), "")
	if err != nil {
		return nil, branchKey, nil, err
	}

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Load only target branch
	if loadConfigs {
		m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.Id))})
	} else {
		m.Filter().SetAllowedKeys([]model.Key{branchKey})
	}
	prj := project.NewWithManifest(d.RequestCtx(), fs, m)

	// Load project state
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
	if err != nil {
		return nil, branchKey, nil, err
	}

	// Copy remote state to the local
	for _, objectState := range prjState.All() {
		objectState.SetLocalState(deepcopy.Copy(objectState.RemoteState()).(model.Object))
	}

	// Check instance existence in metadata
	branch, _ := prjState.GetOrNil(branchKey).(*model.BranchState)
	instance, found, _ := branch.Local.Metadata.TemplateInstance(instanceId)
	if !found {
		return nil, branchKey, nil, &GenericError{
			Name:    "templates.instanceNotFound",
			Message: fmt.Sprintf(`Instance "%s" not found in branch "%d".`, instanceId, branchKey.Id),
		}
	}

	return prjState, branchKey, instance, nil
}

// tryLockProject.
func tryLockProject(d dependencies.ForProjectRequest) (dependencies.UnlockFn, error) {
	d.Logger().Infof(`requested lock for project "%d"`, d.ProjectID())

	// Try lock
	locked, unlockFn := d.ProjectLocker().TryLock(d.RequestCtx(), fmt.Sprintf("project-%d", d.ProjectID()))
	if !locked {
		d.Logger().Infof(`project "%d" is locked by another request`, d.ProjectID())
		return nil, &ProjectLockedError{
			StatusCode: http.StatusServiceUnavailable,
			Name:       "templates.projectLocked",
			Message:    "The project is locked, another operation is in progress, please try again later.",
			RetryAfter: time.Now().Add(ProjectLockedRetryAfter).UTC().Format(http.TimeFormat),
		}
	}

	// Locked!
	return unlockFn, nil
}
