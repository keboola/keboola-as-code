package dependencies

import (
	"context"
	"fmt"
	"io"

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

// provider implements Provider interface.
type provider struct {
	commandCtx context.Context
	logger     log.Logger
	proc       *servicectx.Process
	fs         filesystem.Fs
	dialogs    *dialog.Dialogs
	options    *options.Options
	envs       *env.Map
	stdout     io.Writer
	stderr     io.Writer
	verbose    bool
	verboseAPI bool

	baseScp      dependencies.Lazy[*baseScope]
	localCmdScp  dependencies.Lazy[*localCommandScope]
	remoteCmdScp dependencies.Lazy[*remoteCommandScope]
}

type _provider Provider

// ProviderRef is a helper to pass a reference to a Provider that will be set later.
type ProviderRef struct {
	_provider
}

func (r *ProviderRef) Set(provider Provider) {
	r._provider = provider
}

func NewProvider(
	commandCtx context.Context,
	logger log.Logger,
	proc *servicectx.Process,
	fs filesystem.Fs,
	dialogs *dialog.Dialogs,
	envs *env.Map,
	stdout io.Writer,
	stderr io.Writer,
	verbose bool,
	verboseAPI bool,
) Provider {
	return &provider{
		commandCtx: commandCtx,
		logger:     logger,
		proc:       proc,
		fs:         fs,
		dialogs:    dialogs,
		envs:       envs,
		stdout:     stdout,
		stderr:     stderr,
		verboseAPI: verboseAPI,
		verbose:    verbose,
	}
}

func (v *provider) BaseScope() BaseScope {
	return v.baseScp.MustInitAndGet(func() *baseScope {
		// Create base HTTP client for all API requests to other APIs
		httpClient := httpclient.New(
			httpclient.WithUserAgent(fmt.Sprintf("keboola-cli/%s", build.BuildVersion)),
			func(c *httpclient.Config) {
				if v.verbose {
					httpclient.WithDebugOutput(v.stdout)(c)
				}
				if v.verboseAPI {
					httpclient.WithDumpOutput(v.stdout)(c)
				}
			},
		)
		return newBaseScope(v.commandCtx, v.logger, v.stdout, v.stderr, v.proc, httpClient, v.fs, v.dialogs, v.options, v.envs)
	})
}

func (v *provider) LocalCommandScope(ctx context.Context, opts ...Option) (LocalCommandScope, error) {
	return v.localCmdScp.InitAndGet(func() (*localCommandScope, error) {
		return newLocalCommandScope(ctx, v.BaseScope(), opts...)
	})
}

func (v *provider) RemoteCommandScope(ctx context.Context, opts ...Option) (RemoteCommandScope, error) {
	return v.remoteCmdScp.InitAndGet(func() (*remoteCommandScope, error) {
		localScope, err := v.LocalCommandScope(ctx)
		if err != nil {
			return nil, err
		}

		remoteScope, err := newRemoteCommandScope(ctx, localScope, opts...)
		if err != nil {
			return nil, err
		}

		return remoteScope, nil
	})
}

func (v *provider) LocalProject(ctx context.Context, ignoreErrors bool, ops ...Option) (*project.Project, RemoteCommandScope, error) {
	// Get local scope
	localCmdScp, err := v.LocalCommandScope(ctx, ops...)
	if err != nil {
		return nil, nil, err
	}

	prj, _, err := localCmdScp.LocalProject(ctx, ignoreErrors)
	if err != nil {
		return nil, nil, err
	}

	// Authentication
	remoteCmdScp, err := v.RemoteCommandScope(ctx)
	if err != nil {
		return nil, nil, err
	}

	return prj, remoteCmdScp, nil
}

func (v *provider) LocalRepository(ctx context.Context, ops ...Option) (*repository.Repository, LocalCommandScope, error) {
	// Get local repository
	localCmdScp, err := v.LocalCommandScope(ctx, ops...)
	if err != nil {
		return nil, nil, err
	}

	repo, _, err := localCmdScp.LocalTemplateRepository(ctx)
	if err != nil {
		return nil, nil, err
	}

	return repo, localCmdScp, nil
}

func (v *provider) LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error) {
	return v.BaseScope().LocalDbtProject(ctx)
}
