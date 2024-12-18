package service

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	deleteTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
	renameInst "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const (
	ProjectLockedRetryAfter = 5 * time.Second
	TemplateUpgradeTaskType = "template.upgrade"
	TemplateUseTaskType     = "template.use"
	TemplatePreviewTaskType = "template.preview"
	TemplateDeleteTaskType  = "template.delete"
)

type service struct {
	config config.Config
	deps   dependencies.APIScope
	tasks  *task.Node
	mapper *Mapper
}

func New(ctx context.Context, d dependencies.APIScope) (Service, error) {
	if err := StartComponentsCron(ctx, d); err != nil {
		return nil, err
	}

	if err := StartRepositoriesPullCron(ctx, d); err != nil {
		return nil, err
	}

	s := &service{
		config: d.APIConfig(),
		deps:   d,
		tasks:  d.TaskNode(),
		mapper: NewMapper(d),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background()) // nolint: contextcheck
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		d.Logger().Info(ctx, "received shutdown request")
		cancel()
		d.Logger().Info(ctx, "waiting for orchestrators")
		wg.Wait()
		d.Logger().Info(ctx, "shutdown done")
	})

	return s, nil
}

func (s *service) APIRootIndex(context.Context, dependencies.PublicRequestScope) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(context.Context, dependencies.PublicRequestScope) (res *ServiceDetail, err error) {
	url := *s.deps.APIConfig().API.PublicURL
	url.Path = path.Join(url.Path, "v1/documentation")
	res = &ServiceDetail{
		API:           "templates",
		Documentation: url.String(),
	}
	return res, nil
}

func (s *service) HealthCheck(context.Context, dependencies.PublicRequestScope) (res string, err error) {
	return "OK", nil
}

func (s *service) RepositoriesIndex(ctx context.Context, d dependencies.ProjectRequestScope, _ *RepositoriesIndexPayload) (res *Repositories, err error) {
	return RepositoriesResponse(ctx, d)
}

func (s *service) RepositoryIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *RepositoryIndexPayload) (res *Repository, err error) {
	repo, err := repositoryInst(ctx, d, payload.Repository)
	if err != nil {
		return nil, err
	}
	return RepositoryResponse(ctx, d, repo), nil
}

func (s *service) TemplatesIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *TemplatesIndexPayload) (res *Templates, err error) {
	repo, err := repositoryInst(ctx, d, payload.Repository)
	if err != nil {
		return nil, err
	}
	return TemplatesResponse(ctx, d, repo, repo.Templates(), payload.Filter)
}

func (s *service) TemplateIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *TemplateIndexPayload) (res *TemplateDetail, err error) {
	repo, tmplRecord, err := templateRecord(ctx, d, payload.Repository, payload.Template)
	if err != nil {
		return nil, err
	}
	return TemplateDetailResponse(ctx, d, repo, tmplRecord)
}

func (s *service) VersionIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *VersionIndexPayload) (res *VersionDetailExtended, err error) {
	repo, tmpl, err := getTemplateVersion(ctx, d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return VersionDetailExtendedResponse(ctx, d, repo, tmpl)
}

func (s *service) InputsIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *InputsIndexPayload) (res *Inputs, err error) {
	_, tmpl, err := getTemplateVersion(ctx, d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}
	return InputsResponse(ctx, d, tmpl.Inputs().ToExtended()), nil
}

func (s *service) ValidateInputs(ctx context.Context, d dependencies.ProjectRequestScope, payload *ValidateInputsPayload) (res *ValidationResult, err error) {
	_, tmpl, err := getTemplateVersion(ctx, d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, _, err := validateInputs(ctx, tmpl.Inputs(), payload.Steps)
	return result, err
}

func (s *service) Preview(ctx context.Context, d dependencies.ProjectRequestScope, payload *PreviewPayload) (res *Task, err error) {
	// Lock project
	unlockFn, err := tryLockProject(ctx, d)
	if err != nil {
		return nil, err
	}
	defer unlockFn(ctx)

	branchKey, err := getBranch(ctx, d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(ctx, d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, values, err := validateInputs(ctx, tmpl.Inputs(), payload.Steps)
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

	tKey := task.Key{
		ProjectID: d.ProjectID(),
		TaskID:    task.ID(TemplatePreviewTaskType),
	}

	t, err := s.tasks.StartTask(ctx, task.Config{
		Type: TemplatePreviewTaskType,
		Key:  tKey,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Create virtual fs, after refactoring it will be removed
			fs := aferofs.NewMemoryFs(filesystem.WithLogger(d.Logger()))

			// Create fake manifest
			m := project.NewManifest(123, "foo")

			// Load all from the target branch, we need shared codes
			m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.ID))})
			prj := project.NewWithManifest(ctx, fs, m)

			// Load project state
			prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
			if err != nil {
				return task.ErrResult(err)
			}

			// Copy remote state to the local
			for _, objectState := range prjState.All() {
				objectState.SetLocalState(deepcopy.Copy(objectState.RemoteState()).(model.Object))
			}

			// Options
			options := preview.Options{
				ConfigName:   payload.Name,
				TargetBranch: branchKey,
				Inputs:       values,
			}

			// Use template
			opResult, err := generateTemplatePreview(ctx, tmpl, d, prjState, options)
			if err != nil {
				return task.ErrResult(err)
			}

			// Push changes
			changeDesc := fmt.Sprintf("From template %s", tmpl.FullName())
			if err := push.Run(ctx, prjState, push.Options{ChangeDescription: changeDesc, SkipValidation: true}, d); err != nil {
				return task.ErrResult(err)
			}

			return task.
				OkResult(`template preview created`).
				WithOutput("configId", opResult.ConfigID)
		},
	})
	if err != nil {
		return nil, err
	}

	task, err := s.mapper.TaskPayload(t)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (s *service) UseTemplateVersion(ctx context.Context, d dependencies.ProjectRequestScope, payload *UseTemplateVersionPayload) (res *Task, err error) {
	// Lock project
	unlockFn, err := tryLockProject(ctx, d)
	if err != nil {
		return nil, err
	}
	defer unlockFn(ctx)

	// Note:
	//   A very strange code follows.
	//   Since I did not manage to complete the refactoring - separation of remote and local state.
	//   A virtual FS and fake manifest is created to make it work.

	branchKey, err := getBranch(ctx, d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(ctx, d, payload.Repository, payload.Template, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, values, err := validateInputs(ctx, tmpl.Inputs(), payload.Steps)
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

	tKey := task.Key{
		ProjectID: d.ProjectID(),
		TaskID:    task.ID(TemplateUseTaskType),
	}

	t, err := s.tasks.StartTask(ctx, task.Config{
		Type: TemplateUseTaskType,
		Key:  tKey,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Create virtual fs, after refactoring it will be removed
			fs := aferofs.NewMemoryFs(filesystem.WithLogger(d.Logger()))

			// Create fake manifest
			m := project.NewManifest(123, "foo")

			// Load all from the target branch, we need shared codes
			m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.ID))})
			prj := project.NewWithManifest(ctx, fs, m)

			// Load project state
			prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
			if err != nil {
				return task.ErrResult(err)
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
			opResult, err := useTemplate.Run(ctx, prjState, tmpl, options, d)
			if err != nil {
				return task.ErrResult(err)
			}

			// Push changes
			changeDesc := fmt.Sprintf("From template %s", tmpl.FullName())
			if err := push.Run(ctx, prjState, push.Options{ChangeDescription: changeDesc, SkipValidation: true}, d); err != nil {
				return task.ErrResult(err)
			}

			return task.
				OkResult(fmt.Sprintf(`template instance with id "%s" created`, opResult.InstanceID)).
				WithOutput("instanceId", opResult.InstanceID)
		},
	})
	if err != nil {
		return nil, err
	}

	task, err := s.mapper.TaskPayload(t)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (s *service) InstancesIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *InstancesIndexPayload) (res *Instances, err error) {
	branchKey, err := getBranch(ctx, d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Create virtual fs, after refactoring it will be removed
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(d.Logger()))

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Only one branch
	m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.ID))})
	prj := project.NewWithManifest(ctx, fs, m)

	// Load project state
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
	if err != nil {
		return nil, err
	}

	return InstancesResponse(ctx, d, prjState, branchKey)
}

func (s *service) InstanceIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *InstanceIndexPayload) (res *InstanceDetail, err error) {
	_, span := d.Telemetry().Tracer().Start(ctx, "api.server.templates.service.InstanceIndex")
	defer span.End(&err)

	branchKey, err := getBranch(ctx, d, payload.Branch)
	if err != nil {
		return nil, err
	}

	// Create virtual fs, after refactoring it will be removed
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(d.Logger()))

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Only one branch
	m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.ID))})
	prj := project.NewWithManifest(ctx, fs, m)

	// Load project state
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, d)
	if err != nil {
		return nil, err
	}
	return InstanceResponse(ctx, d, prjState, branchKey, payload.InstanceID)
}

func (s *service) UpdateInstance(ctx context.Context, d dependencies.ProjectRequestScope, payload *UpdateInstancePayload) (res *InstanceDetail, err error) {
	// Lock project
	unlockFn, err := tryLockProject(ctx, d)
	if err != nil {
		return nil, err
	}
	defer unlockFn(ctx)

	// Get instance
	prjState, branchKey, instance, err := getTemplateInstance(ctx, d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	opts := renameInst.Options{
		Branch:   branchKey,
		Instance: *instance,
		NewName:  payload.Name,
	}

	err = renameInst.Run(ctx, prjState, opts, d)
	if err != nil {
		return nil, err
	}

	// Push changes
	changeDesc := fmt.Sprintf("Rename template instance %s", payload.InstanceID)
	if err := push.Run(ctx, prjState, push.Options{ChangeDescription: changeDesc, AllowRemoteDelete: true, DryRun: false, SkipValidation: true}, d); err != nil {
		return nil, err
	}

	return InstanceResponse(ctx, d, prjState, branchKey, payload.InstanceID)
}

func (s *service) DeleteInstance(ctx context.Context, d dependencies.ProjectRequestScope, payload *DeleteInstancePayload) (*Task, error) {
	// Lock project
	unlockFn, err := tryLockProject(ctx, d)
	if err != nil {
		return nil, err
	}
	defer unlockFn(ctx)

	// Get instance
	prjState, branchKey, _, err := getTemplateInstance(ctx, d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	// Delete template instance
	deleteOpts := deleteTemplate.Options{
		Branch:   branchKey,
		DryRun:   false,
		Instance: payload.InstanceID,
	}

	taskKey := task.Key{
		ProjectID: d.ProjectID(),
		TaskID:    task.ID(TemplateDeleteTaskType),
	}

	t, err := s.tasks.StartTask(ctx, task.Config{
		Type: TemplateDeleteTaskType,
		Key:  taskKey,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Minute*5)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			err = deleteTemplate.Run(ctx, prjState, deleteOpts, d)
			if err != nil {
				return task.ErrResult(err)
			}

			// Push changes
			changeDesc := fmt.Sprintf("Delete template instance %s", payload.InstanceID)
			if err := push.Run(ctx, prjState, push.Options{ChangeDescription: changeDesc, AllowRemoteDelete: true, DryRun: false, SkipValidation: true}, d); err != nil {
				return task.ErrResult(err)
			}

			return task.OkResult(fmt.Sprintf(`"template instance with id "%s" is deleted"`, deleteOpts.Instance)).
				WithOutput("instanceId", deleteOpts.Instance)
		},
	})
	if err != nil {
		return nil, err
	}

	task, err := s.mapper.TaskPayload(t)
	if err != nil {
		return nil, err
	}

	return task, err
}

func (s *service) UpgradeInstance(ctx context.Context, d dependencies.ProjectRequestScope, payload *UpgradeInstancePayload) (res *Task, err error) {
	// Lock project
	unlockFn, err := tryLockProject(ctx, d)
	if err != nil {
		return nil, err
	}
	defer unlockFn(ctx)

	// Get instance
	prjState, branchKey, instance, err := getTemplateInstance(ctx, d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(ctx, d, instance.RepositoryName, instance.TemplateID, payload.Version)
	if err != nil {
		return nil, err
	}

	// Process inputs
	result, values, err := validateInputs(ctx, tmpl.Inputs(), payload.Steps)
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

	tKey := task.Key{
		ProjectID: d.ProjectID(),
		TaskID:    task.ID(TemplateUpgradeTaskType),
	}

	t, err := s.tasks.StartTask(ctx, task.Config{
		Type: TemplateUpgradeTaskType,
		Key:  tKey,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Upgrade template instance
			upgradeOpts := upgradeTemplate.Options{
				Branch:   branchKey,
				Instance: *instance,
				Inputs:   values,
			}
			_, err = upgradeTemplate.Run(ctx, prjState, tmpl, upgradeOpts, d)
			if err != nil {
				return task.ErrResult(err)
			}

			// Push changes
			changeDesc := fmt.Sprintf("Upgraded from template %s", tmpl.FullName())
			if err := push.Run(ctx, prjState, push.Options{ChangeDescription: changeDesc, AllowRemoteDelete: true, DryRun: false, SkipValidation: true}, d); err != nil {
				return task.ErrResult(err)
			}

			return task.
				OkResult(fmt.Sprintf(`template instance with id "%s" upgraded`, instance.InstanceID)).
				WithOutput("instanceId", instance.InstanceID)
		},
	})
	if err != nil {
		return nil, err
	}

	task, err := s.mapper.TaskPayload(t)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (s *service) UpgradeInstanceInputsIndex(ctx context.Context, d dependencies.ProjectRequestScope, payload *UpgradeInstanceInputsIndexPayload) (res *Inputs, err error) {
	// Get instance
	prjState, branchKey, instance, err := getTemplateInstance(ctx, d, payload.Branch, payload.InstanceID, true)
	if err != nil {
		return nil, err
	}

	// Get template
	_, tmpl, err := getTemplateVersion(ctx, d, instance.RepositoryName, instance.TemplateID, payload.Version)
	if err != nil {
		return nil, err
	}

	// Generate response
	return UpgradeInstanceInputsResponse(ctx, d, prjState, branchKey, instance, tmpl), nil
}

func (s *service) UpgradeInstanceValidateInputs(ctx context.Context, d dependencies.ProjectRequestScope, payload *UpgradeInstanceValidateInputsPayload) (res *ValidationResult, err error) {
	// Get instance
	_, _, instance, err := getTemplateInstance(ctx, d, payload.Branch, payload.InstanceID, false)
	if err != nil {
		return nil, err
	}

	// Validate the inputs as in the use operation
	return s.ValidateInputs(ctx, d, &ValidateInputsPayload{
		Repository:      instance.RepositoryName,
		Template:        instance.TemplateID,
		Version:         payload.Version,
		Steps:           payload.Steps,
		StorageAPIToken: payload.StorageAPIToken,
	})
}

func (s *service) GetTask(ctx context.Context, d dependencies.ProjectRequestScope, payload *GetTaskPayload) (res *Task, err error) {
	t, err := s.tasks.GetTask(task.Key{ProjectID: d.ProjectID(), TaskID: payload.TaskID}).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	task, err := s.mapper.TaskPayload(t)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func repositoryRef(d dependencies.ProjectRequestScope, name string) (model.TemplateRepository, error) {
	if repo, found := d.ProjectRepositories().Get(name); found {
		return repo, nil
	} else {
		return model.TemplateRepository{}, &GenericError{
			StatusCode: http.StatusNotFound,
			Name:       "templates.repositoryNotFound",
			Message:    fmt.Sprintf(`Repository "%s" not found.`, name),
		}
	}
}

func repositoryInst(ctx context.Context, d dependencies.ProjectRequestScope, repoName string) (*repository.Repository, error) {
	// Get repository ref
	repoRef, err := repositoryRef(d, repoName)
	if err != nil {
		return nil, err
	}

	// Get repository
	repo, err := d.TemplateRepository(ctx, repoRef)
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func templateRecord(ctx context.Context, d dependencies.ProjectRequestScope, repoName, templateID string) (*repository.Repository, *repository.TemplateRecord, error) {
	// Get repository
	repo, err := repositoryInst(ctx, d, repoName)
	if err != nil {
		return nil, nil, err
	}

	// Get template record
	tmpl, found := repo.RecordByID(templateID)
	if !found {
		return nil, nil, &GenericError{
			StatusCode: http.StatusNotFound,
			Name:       "templates.templateNotFound",
			Message:    fmt.Sprintf(`Template "%s" not found.`, templateID),
		}
	}
	return repo, &tmpl, nil
}

func getTemplateVersion(ctx context.Context, d dependencies.ProjectRequestScope, repoName, templateID, versionStr string) (*repository.Repository, *template.Template, error) {
	// Get repo
	repo, err := repositoryInst(ctx, d, repoName)
	if err != nil {
		return nil, nil, err
	}

	tmplRecord, found := repo.RecordByID(templateID)

	if !found {
		return nil, nil, &GenericError{
			StatusCode: http.StatusNotFound,
			Name:       "templates.templateNotFound",
			Message:    fmt.Sprintf(`Template "%s" not found.`, templateID),
		}
	}

	if !hasRequirements(tmplRecord, d) {
		return nil, nil, &GenericError{
			StatusCode: http.StatusBadRequest,
			Name:       "templates.templateNoRequirements",
			Message:    fmt.Sprintf(`Template "%s" does not met requirements because of project misconfiguration.`, templateID),
		}
	}

	// Parse version
	var semVersion model.SemVersion
	if versionStr == "default" {
		// Default version
		if versionRecord, err := tmplRecord.DefaultVersionOrErr(); err != nil {
			return nil, nil, &GenericError{
				StatusCode: http.StatusNotFound,
				Name:       "templates.templateNotFound",
				Message:    err.Error(),
			}
		} else {
			semVersion = versionRecord.Version
		}
	} else if v, err := model.NewSemVersion(versionStr); err != nil {
		// Invalid semantic version
		return nil, nil, NewBadRequestError(errors.Errorf(`version "%s" is not valid: %s`, versionStr, err))
	} else {
		// Parsed version
		semVersion = v
	}

	// Get template version
	tmpl, err := d.Template(ctx, model.NewTemplateRef(repo.Definition(), templateID, semVersion.Original()))
	if err != nil {
		if errors.As(err, &manifest.TemplateNotFoundError{}) {
			return nil, nil, &GenericError{
				StatusCode: http.StatusNotFound,
				Name:       "templates.templateNotFound",
				Message:    fmt.Sprintf(`Template "%s" not found.`, templateID),
			}
		}
		if errors.As(err, &manifest.VersionNotFoundError{}) {
			return nil, nil, &GenericError{
				StatusCode: http.StatusNotFound,
				Name:       "templates.versionNotFound",
				Message:    fmt.Sprintf(`Version "%s" not found.`, versionStr),
			}
		}
		return nil, nil, err
	}

	return repo, tmpl, nil
}

func getBranch(ctx context.Context, d dependencies.ProjectRequestScope, branchDef string) (model.BranchKey, error) {
	// Get Keboola API
	api := d.KeboolaProjectAPI()

	// Parse branch ID
	var targetBranch model.BranchKey
	if branchDef == "default" {
		// Use main branch
		if v, err := api.GetDefaultBranchRequest().Send(ctx); err != nil {
			return targetBranch, err
		} else {
			targetBranch.ID = v.ID
		}
	} else if branchID, err := strconv.Atoi(branchDef); err != nil {
		// Branch ID must be numeric
		return targetBranch, NewBadRequestError(errors.Errorf(`branch ID "%s" is not numeric`, branchDef))
	} else if _, err := api.GetBranchRequest(keboola.BranchKey{ID: keboola.BranchID(branchID)}).Send(ctx); err != nil {
		// Branch not found
		return targetBranch, NewResourceNotFoundError("branch", strconv.Itoa(branchID), "project")
	} else {
		// Branch found
		targetBranch.ID = keboola.BranchID(branchID)
	}

	return targetBranch, nil
}

func getTemplateInstance(ctx context.Context, d dependencies.ProjectRequestScope, branchDef, instanceId string, loadConfigs bool) (*project.State, model.BranchKey, *model.TemplateInstance, error) {
	// Note:
	//   Waits for separation of remote and local state.
	//   A virtual FS and fake manifest are created to make it work.

	branchKey, err := getBranch(ctx, d, branchDef)
	if err != nil {
		return nil, branchKey, nil, err
	}

	// Create virtual fs, after refactoring it will be removed
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(d.Logger()))

	// Create fake manifest
	m := project.NewManifest(123, "foo")

	// Load only target branch
	if loadConfigs {
		m.Filter().SetAllowedBranches(model.AllowedBranches{model.AllowedBranch(cast.ToString(branchKey.ID))})
	} else {
		m.Filter().SetAllowedKeys([]model.Key{branchKey})
	}
	prj := project.NewWithManifest(ctx, fs, m)

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
			StatusCode: http.StatusNotFound,
			Name:       "templates.instanceNotFound",
			Message:    fmt.Sprintf(`Instance "%s" not found in branch "%d".`, instanceId, branchKey.ID),
		}
	}
	return prjState, branchKey, instance, nil
}

// tryLockProject.
func tryLockProject(ctx context.Context, d dependencies.ProjectRequestScope) (unlock func(ctx context.Context), err error) {
	d.Logger().Infof(ctx, `requested lock for project "%d"`, d.ProjectID())

	mutex := d.DistributedLockProvider().NewMutex(fmt.Sprintf("project/%d", d.ProjectID()))

	err = mutex.TryLock(ctx)
	if errors.As(err, &etcdop.AlreadyLockedError{}) {
		err = &ProjectLockedError{
			StatusCode: http.StatusServiceUnavailable,
			Name:       "templates.projectLocked",
			Message:    "The project is locked, another operation is in progress, please try again later.",
			RetryAfter: time.Now().Add(ProjectLockedRetryAfter).UTC().Format(http.TimeFormat),
		}
	}
	if err != nil {
		return nil, err
	}

	// Locked!
	unlockFn := func(ctx context.Context) {
		if err := mutex.Unlock(ctx); err != nil {
			d.Logger().Warnf(ctx, `cannot unlock project "%d": %s`, d.ProjectID(), err)
		}
	}
	return unlockFn, nil
}

func generateTemplatePreview(ctx context.Context, tmpl *template.Template, d dependencies.ProjectRequestScope, projectState *project.State, o preview.Options) (result *use.Result, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.preview")
	defer span.End(&err)

	// Create tickets provider, to generate new IDS
	tickets := d.ObjectIDGeneratorFactory()(ctx)

	// Prepare template
	tmplCtx := preview.NewContext(ctx, tmpl.Reference(), tmpl.ObjectsRoot(), o.TargetBranch, o.Inputs, tmpl.Inputs().InputsMap(), tickets, d.Components(), projectState.State(), d.ProjectBackends())
	plan, err := useTemplate.PrepareTemplate(ctx, d, useTemplate.ExtendedOptions{
		TargetBranch:          o.TargetBranch,
		Inputs:                o.Inputs,
		ConfigName:            o.ConfigName,
		ProjectState:          projectState,
		Template:              tmpl,
		TemplateCtx:           tmplCtx,
		Upgrade:               false,
		SkipEncrypt:           o.SkipEncrypt,
		SkipSecretsValidation: o.SkipSecretsValidation,
	})
	if err != nil {
		return nil, err
	}

	return plan.Invoke(ctx)
}
