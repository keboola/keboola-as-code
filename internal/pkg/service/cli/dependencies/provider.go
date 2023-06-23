package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
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

func NewProvider(commandCtx context.Context, logger log.Logger, proc *servicectx.Process, fs filesystem.Fs, dialogs *dialog.Dialogs, opts *options.Options) Provider {
	return &provider{
		commandCtx: commandCtx,
		logger:     logger,
		proc:       proc,
		fs:         fs,
		dialogs:    dialogs,
		options:    opts,
	}
}

func (v *provider) BaseScope() BaseScope {
	return v.baseScp.MustInitAndGet(func() *baseScope {
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
		return newBaseScope(v.commandCtx, v.logger, v.proc, httpClient, v.fs, v.dialogs, v.options)
	})
}

func (v *provider) LocalCommandScope(opts ...Option) (LocalCommandScope, error) {
	return v.localCmdScp.InitAndGet(func() (*localCommandScope, error) {
		return newLocalCommandScope(v.BaseScope(), opts...)
	})
}

func (v *provider) RemoteCommandScope(opts ...Option) (RemoteCommandScope, error) {
	return v.remoteCmdScp.InitAndGet(func() (*remoteCommandScope, error) {
		localScope, err := v.LocalCommandScope()
		if err != nil {
			return nil, err
		}

		remoteScope, err := newRemoteCommandScope(localScope.CommandCtx(), localScope, opts...)
		if err != nil {
			return nil, err
		}

		return remoteScope, nil
	})
}

func (v *provider) LocalProject(ignoreErrors bool, ops ...Option) (*project.Project, RemoteCommandScope, error) {
	// Get local scope
	localCmdScp, err := v.LocalCommandScope(ops...)
	if err != nil {
		return nil, nil, err
	}

	prj, _, err := localCmdScp.LocalProject(ignoreErrors)
	if err != nil {
		return nil, nil, err
	}

	// Authentication
	remoteCmdScp, err := v.RemoteCommandScope()
	if err != nil {
		return nil, nil, err
	}

	return prj, remoteCmdScp, nil
}

func (v *provider) LocalRepository(ops ...Option) (*repository.Repository, LocalCommandScope, error) {
	// Get local repository
	localCmdScp, err := v.LocalCommandScope(ops...)
	if err != nil {
		return nil, nil, err
	}

	repo, _, err := localCmdScp.LocalTemplateRepository(localCmdScp.CommandCtx())
	if err != nil {
		return nil, nil, err
	}

	return repo, localCmdScp, nil
}

func (v *provider) LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error) {
	return v.BaseScope().LocalDbtProject(ctx)
}
