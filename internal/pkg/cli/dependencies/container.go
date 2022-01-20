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
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
