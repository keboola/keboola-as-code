package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

var (
	ErrMissingStorageApiHost  = dialog.ErrMissingStorageApiHost
	ErrMissingStorageApiToken = dialog.ErrMissingStorageApiToken
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
}

type cliDeps struct {
	_all    *Container // link back to all dependencies
	logger  log.Logger
	dialogs *dialog.Dialogs
	options *options.Options
	envs    *env.Map
	fs      filesystem.Fs
}

func (v *cliDeps) Logger() log.Logger {
	return v.logger
}

func (v *cliDeps) Fs() filesystem.Fs {
	return v.fs
}

func (v *cliDeps) BasePath() string {
	return v.fs.BasePath()
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

func (v *cliDeps) ApiVerboseLogs() bool {
	return v.options.VerboseApi
}

func (v *cliDeps) StorageApiHost() (string, error) {
	var host string
	if v._all.LocalProjectExists() {
		if prj, err := v._all.LocalProject(false); err == nil {
			host = prj.ProjectManifest().ApiHost()
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
