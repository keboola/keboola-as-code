package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

// provider implements Provider interface.
type provider struct {
	commandCtx  context.Context
	envs        env.Provider
	logger      log.Logger
	fs          filesystem.Fs
	dialogs     *dialog.Dialogs
	options     *options.Options
	baseDeps    dependencies.Lazy[*base]
	publicDeps  dependencies.Lazy[*local]
	projectDeps dependencies.Lazy[*remote]
}

type _provider Provider

// ProviderRef is a helper to pass a reference to a Provider that will be set later.
type ProviderRef struct {
	_provider
}

func (r *ProviderRef) Set(provider Provider) {
	r._provider = provider
}

func NewProvider(commandCtx context.Context, envs env.Provider, logger log.Logger, fs filesystem.Fs, dialogs *dialog.Dialogs, opts *options.Options) Provider {
	return &provider{
		commandCtx: commandCtx,
		envs:       envs,
		logger:     logger,
		fs:         fs,
		dialogs:    dialogs,
		options:    opts,
	}
}

func (v *provider) BaseDependencies() Base {
	return v.baseDeps.MustInitAndGet(func() *base {
		// Create base HTTP client for all API requests to other APIs
		httpClient := httpclient.New(
			httpclient.WithUserAgent(fmt.Sprintf("keboola-cli/%s", build.BuildVersion)),
			httpclient.WithDebugOutput(v.logger.DebugWriter()),
			func(c *httpclient.Config) {
				if v.options.VerboseAPI {
					httpclient.WithDumpOutput(v.logger.DebugWriter())(c)
				}
			},
		)
		return newBaseDeps(v.commandCtx, v.envs, v.logger, httpClient, v.fs, v.dialogs, v.options)
	})
}

func (v *provider) DependenciesForLocalCommand() (ForLocalCommand, error) {
	return v.publicDeps.InitAndGet(func() (*local, error) {
		return newPublicDeps(v.BaseDependencies())
	})
}

func (v *provider) DependenciesForRemoteCommand() (ForRemoteCommand, error) {
	return v.projectDeps.InitAndGet(func() (*remote, error) {
		publicDeps, err := v.DependenciesForLocalCommand()
		if err != nil {
			return nil, err
		}

		projectDeps, err := newProjectDeps(publicDeps.CommandCtx(), publicDeps)
		if err != nil {
			return nil, err
		}

		return projectDeps, nil
	})
}

func (v *provider) LocalProject(ignoreErrors bool) (*project.Project, ForRemoteCommand, error) {
	// Get local project
	publicDeps, err := v.DependenciesForLocalCommand()
	if err != nil {
		return nil, nil, err
	}
	prj, _, err := publicDeps.LocalProject(ignoreErrors)
	if err != nil {
		return nil, nil, err
	}

	// Authentication
	d, err := v.DependenciesForRemoteCommand()
	if err != nil {
		return nil, nil, err
	}

	return prj, d, nil
}

func (v *provider) LocalRepository() (*repository.Repository, ForLocalCommand, error) {
	// Get local repository
	d, err := v.DependenciesForLocalCommand()
	if err != nil {
		return nil, nil, err
	}
	repo, _, err := d.LocalTemplateRepository(d.CommandCtx())
	if err != nil {
		return nil, nil, err
	}
	return repo, d, nil
}

func (v *provider) LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error) {
	return v.BaseDependencies().LocalDbtProject(ctx)
}
