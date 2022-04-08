package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

var (
	ErrMissingStorageApiHost          = fmt.Errorf(`missing Storage API host`)
	ErrMissingStorageApiToken         = fmt.Errorf(`missing Storage API token`)
	ErrProjectDirFound                = fmt.Errorf("project directory not expected, but found")
	ErrProjectManifestNotFound        = fmt.Errorf("project manifest not found")
	ErrTemplateDirFound               = fmt.Errorf("template directory not expected, but found")
	ErrTemplateManifestNotFound       = fmt.Errorf("template manifest not found")
	ErrRepositoryDirFound             = fmt.Errorf("repository directory not expected, but found")
	ErrRepositoryManifestNotFound     = fmt.Errorf("repository manifest not found")
	ErrExpectedRepositoryFoundProject = fmt.Errorf("repository manifest not found, found project manifest")
	ErrExpectedProjectFoundRepository = fmt.Errorf("project manifest not found, found repository manifest")
	ErrExpectedProjectFoundTemplate   = fmt.Errorf("project manifest not found, found template manifest")
)

// Container provides dependencies used only in the CLI + common dependencies.
type Container interface {
	dependencies.Common
	Ctx() context.Context
	WithCtx(ctx context.Context) Container
	BasePath() string
	EmptyDir() (filesystem.Fs, error)
	Dialogs() *dialog.Dialogs
	Options() *options.Options
	LocalProject(ignoreErrors bool) (*project.Project, error)
	LocalProjectExists() bool
	LocalTemplate() (*template.Template, error)
	LocalTemplateExists() bool
	LocalTemplateRepository() (*repository.Repository, error)
	LocalTemplateRepositoryExists() bool
}

type Provider interface {
	Dependencies() Container
}

func NewContainer(ctx context.Context, envs *env.Map, fs filesystem.Fs, dialogs *dialog.Dialogs, logger log.Logger, options *options.Options) Container {
	c := &container{ctx: ctx, logger: logger, envs: envs, fs: fs, dialogs: dialogs, options: options}
	c.commonDeps = dependencies.NewCommonContainer(c)
	return c
}

type commonDeps = dependencies.Common

type container struct {
	commonDeps
	ctx        context.Context
	logger     log.Logger
	dialogs    *dialog.Dialogs
	options    *options.Options
	envs       *env.Map
	storageApi *storageapi.Api
	// Fs
	fs       filesystem.Fs
	emptyDir filesystem.Fs
	// Project
	project    *project.Project
	projectDir filesystem.Fs
}

func (v *container) Ctx() context.Context {
	return v.ctx
}

func (v *container) WithCtx(ctx context.Context) Container {
	clone := *v
	clone.ctx = ctx
	return &clone
}

func (v *container) Logger() log.Logger {
	return v.logger
}

func (v *container) BasePath() string {
	return v.fs.BasePath()
}

func (v *container) Envs() *env.Map {
	return v.envs
}

func (v *container) Dialogs() *dialog.Dialogs {
	return v.dialogs
}

func (v *container) Options() *options.Options {
	return v.options
}

func (v *container) ApiVerboseLogs() bool {
	return v.options.VerboseApi
}

func (v *container) StorageApiHost() (string, error) {
	var host string
	if v.LocalProjectExists() {
		if prj, err := v.LocalProject(false); err == nil {
			host = prj.ProjectManifest().ApiHost()
		} else {
			return "", err
		}
	} else {
		host = v.options.GetString(options.StorageApiHostOpt)
	}

	host = strhelper.NormalizeHost(host)
	if host == "" {
		return "", ErrMissingStorageApiHost
	}
	return host, nil
}

func (v *container) StorageApiToken() (string, error) {
	token := v.options.GetString(options.StorageApiTokenOpt)
	if token == "" {
		return "", ErrMissingStorageApiToken
	}
	return token, nil
}

func (v *container) SetStorageApiHost(host string) {
	v.options.Set(`storage-api-host`, host)
}

func (v *container) SetStorageApiToken(host string) {
	v.options.Set(`storage-api-token`, host)
}

func (v *container) StorageApi() (*storageapi.Api, error) {
	if v.storageApi == nil {
		storageApi, err := v.commonDeps.StorageApi()
		if err != nil {
			return nil, err
		}

		// Storage Api token project ID and manifest project ID must be same
		if v.LocalProjectExists() {
			prj, err := v.LocalProject(false)
			if err != nil {
				return nil, err
			}
			projectManifest := prj.ProjectManifest()
			if projectManifest != nil && projectManifest.ProjectId() != storageApi.ProjectId() {
				return nil, fmt.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, storageApi.ProjectId(), projectManifest.ProjectId())
			}
		}

		v.storageApi = storageApi
	}

	return v.storageApi, nil
}
