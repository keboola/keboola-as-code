package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	createProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	loadProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/load"
	createRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/create"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

var (
	ErrMissingStorageApiHost          = dialog.ErrMissingStorageApiHost
	ErrMissingStorageApiToken         = dialog.ErrMissingStorageApiToken
	ErrProjectManifestNotFound        = fmt.Errorf("project manifest not found")
	ErrRepositoryManifestNotFound     = fmt.Errorf("repository manifest not found")
	ErrExpectedProjectFoundRepository = fmt.Errorf("project manifest not found, found repository manifest")
	ErrExpectedRepositoryFoundProject = fmt.Errorf("repository manifest not found, found project manifest")
	ErrProjectDirFound                = fmt.Errorf("project directory not expected, but found")
	ErrRepoDirFound                   = fmt.Errorf("repository directory not expected, but found")
	ErrDirIsNotEmpty                  = fmt.Errorf("dir is not empty")
)

func NewContainer(ctx context.Context, envs *env.Map, fs filesystem.Fs, dialogs *dialog.Dialogs, logger log.Logger, options *options.Options) *Container {
	cli := &cliDeps{logger: logger, envs: envs, fs: fs, dialogs: dialogs, options: options}
	all := &Container{commonDeps: dependencies.NewContainer(cli, ctx), cliDeps: cli}
	cli._all = all
	return all
}

type commonDeps = dependencies.CommonDeps

type Container struct {
	commonDeps
	*cliDeps
	storageApi *remote.StorageApi // see StorageApi(), projectId check
}

type cliDeps struct {
	_all    *Container // link back to all dependencies
	logger  log.Logger
	dialogs *dialog.Dialogs
	options *options.Options
	envs    *env.Map
	// Filesystem
	fs       filesystem.Fs
	emptyDir filesystem.Fs
	// Project
	project         *project.Project
	projectDir      filesystem.Fs
	projectManifest *project.Manifest
	// Template
	template *template.Template
	// Template repository
	templateRepository         *repository.Repository
	templateRepositoryDir      filesystem.Fs
	templateRepositoryManifest *repository.Manifest
}

func (v *cliDeps) Logger() log.Logger {
	return v.logger
}

func (v *cliDeps) Envs() *env.Map {
	return v.envs
}

func (v *cliDeps) Dialogs() *dialog.Dialogs {
	return v.dialogs
}

func (v *cliDeps) Options() *options.Options {
	return v.options
}

func (v *cliDeps) BasePath() string {
	return v.fs.BasePath()
}

func (v *cliDeps) EmptyDir() (filesystem.Fs, error) {
	if v.emptyDir == nil {
		// Project dir is not expected
		if v.ProjectManifestExists() {
			return nil, ErrProjectDirFound
		}

		// Repository dir is not expected
		if v.TemplateRepositoryManifestExists() {
			return nil, ErrRepoDirFound
		}

		// Read directory
		items, err := v.fs.ReadDir(`.`)
		if err != nil {
			return nil, err
		}

		// Filter out ignored files
		found := utils.NewMultiError()
		for _, item := range items {
			if !filesystem.IsIgnoredPath(item.Name(), item) {
				path := item.Name()
				if found.Len() > 5 {
					found.Append(fmt.Errorf(path + ` ...`))
					break
				} else {
					found.Append(fmt.Errorf(path))
				}
			}
		}

		// Directory must be empty
		if found.Len() > 0 {
			return nil, utils.PrefixError(fmt.Sprintf(`directory "%s" it not empty, found`, v.fs.BasePath()), found)
		}

		v.emptyDir = v.fs
	}

	return v.emptyDir, nil
}

func (v *cliDeps) Project() (*project.Project, error) {
	if v.project == nil {
		projectDir, err := v.ProjectDir()
		if err != nil {
			return nil, err
		}
		manifest, err := v.ProjectManifest()
		if err != nil {
			return nil, err
		}
		v.project = project.New(projectDir, manifest, v._all)
	}
	return v.project, nil
}

func (v *cliDeps) ProjectDir() (filesystem.Fs, error) {
	if v.projectDir == nil {
		if !v.ProjectManifestExists() {
			if v.TemplateRepositoryManifestExists() {
				return nil, ErrExpectedProjectFoundRepository
			}
			return nil, ErrProjectManifestNotFound
		}

		// Check version field
		if err := version.CheckLocalVersion(v.logger, v.fs, projectManifest.Path()); err != nil {
			return nil, err
		}

		v.projectDir = v.fs
	}
	return v.projectDir, nil
}

func (v *cliDeps) ProjectManifestExists() bool {
	if v.projectManifest != nil {
		return true
	}
	path := filesystem.Join(filesystem.MetadataDir, projectManifest.FileName)
	return v.fs.IsFile(path)
}

func (v *cliDeps) ProjectManifest() (*project.Manifest, error) {
	if v.projectManifest == nil {
		if m, err := loadProjectManifest.Run(v); err == nil {
			v.projectManifest = m
		} else {
			return nil, err
		}
	}
	return v.projectManifest, nil
}

func (v *cliDeps) Template() (*template.Template, error) {
	if v.template == nil {
		panic(`TODO`)
	}
	return v.template, nil
}

func (v *cliDeps) TemplateRepository() (*repository.Repository, error) {
	if v.templateRepository == nil {
		templateDir, err := v.TemplateRepositoryDir()
		if err != nil {
			return nil, err
		}
		manifest, err := v.TemplateRepositoryManifest()
		if err != nil {
			return nil, err
		}
		v.templateRepository = repository.New(templateDir, manifest)
	}
	return v.templateRepository, nil
}

func (v *cliDeps) TemplateRepositoryDir() (filesystem.Fs, error) {
	if v.templateRepositoryDir == nil {
		if !v.TemplateRepositoryManifestExists() {
			if v.ProjectManifestExists() {
				return nil, ErrExpectedRepositoryFoundProject
			}
			return nil, ErrRepositoryManifestNotFound
		}

		v.templateRepositoryDir = v.fs
	}
	return v.templateRepositoryDir, nil
}

func (v *cliDeps) TemplateRepositoryManifestExists() bool {
	if v.templateRepositoryManifest != nil {
		return true
	}
	path := filesystem.Join(filesystem.MetadataDir, repositoryManifest.FileName)
	return v.fs.IsFile(path)
}

func (v *cliDeps) TemplateRepositoryManifest() (*repository.Manifest, error) {
	if v.projectManifest == nil {
		if m, err := loadRepositoryManifest.Run(v); err == nil {
			v.templateRepositoryManifest = m
		} else {
			return nil, err
		}
	}
	return v.templateRepositoryManifest, nil
}

func (v *cliDeps) CreateTemplateRepositoryManifest() (*repository.Manifest, error) {
	if m, err := createRepositoryManifest.Run(v); err == nil {
		v.templateRepositoryManifest = m
		v.templateRepositoryDir = v.fs
		v.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}

func (v *cliDeps) ApiVerboseLogs() bool {
	return v.options.VerboseApi
}

func (v *cliDeps) StorageApiHost() (string, error) {
	var host string
	if v.ProjectManifestExists() {
		if m, err := v.ProjectManifest(); err == nil {
			host = m.ApiHost()
		} else {
			return "", err
		}
	} else {
		host = v.options.GetString(options.StorageApiHostOpt)
	}
	if host == "" {
		return "", ErrMissingStorageApiHost
	}
	return host, nil
}

func (v *cliDeps) StorageApiToken() (string, error) {
	token := v.options.GetString(options.StorageApiTokenOpt)
	if token == "" {
		return "", ErrMissingStorageApiToken
	}
	return token, nil
}

func (v *cliDeps) SetStorageApiHost(host string) {
	v.options.Set(`storage-api-host`, host)
}

func (v *cliDeps) SetStorageApiToken(host string) {
	v.options.Set(`storage-api-token`, host)
}

func (v *Container) StorageApi() (*remote.StorageApi, error) {
	if v.storageApi == nil {
		storageApi, err := v.commonDeps.StorageApi()
		if err != nil {
			return nil, err
		}

		// Token and manifest project ID must be same
		if v.projectManifest != nil && v.projectManifest.ProjectId() != storageApi.ProjectId() {
			return nil, fmt.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, storageApi.ProjectId(), v.projectManifest.ProjectId())
		}
		v.storageApi = storageApi
	}

	return v.storageApi, nil
}

func (v *Container) CreateProjectManifest(o createProjectManifest.Options) (*project.Manifest, error) {
	if m, err := createProjectManifest.Run(o, v); err == nil {
		v.projectManifest = m
		v.projectDir = v.fs
		v.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}
