package dependencies

import (
	"context"
	"io"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	userAgent = "keboola-stream"
)

// serviceScope implements ServiceScope interface.
type serviceScope struct {
	parentScopes
	hookRegistry         *hook.Registry
	hookExecutor         *hook.Executor
	definitionRepository *definitionRepo.Repository
}

type parentScopes interface {
	dependencies.BaseScope
	dependencies.PublicScope
	dependencies.EtcdClientScope
	dependencies.TaskScope
}

type parentScopesImpl struct {
	dependencies.BaseScope
	dependencies.PublicScope
	dependencies.EtcdClientScope
	dependencies.TaskScope
	dependencies.DistributionScope
}

func NewServiceScope(
	ctx context.Context,
	cfg config.Config,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v ServiceScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.dependencies.NewServiceScope")
	defer span.End(&err)
	parentSc, err := newParentScopes(ctx, cfg, proc, logger, tel, stdout, stderr)
	if err != nil {
		return nil, err
	}
	return newServiceScope(parentSc), nil
}

func newParentScopes(
	ctx context.Context,
	cfg config.Config,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v parentScopes, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.dependencies.newParentScopes")
	defer span.End(&err)

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		httpclient.WithTelemetry(tel),
		httpclient.WithUserAgent(userAgent),
		func(c *httpclient.Config) {
			if cfg.DebugLog {
				httpclient.WithDebugOutput(stdout)(c)
			}
			if cfg.DebugHTTPClient {
				httpclient.WithDumpOutput(stdout)(c)
			}
		},
	)

	d := &parentScopesImpl{}

	d.BaseScope = dependencies.NewBaseScope(ctx, logger, tel, stdout, stderr, clock.New(), proc, httpClient)

	d.PublicScope, err = dependencies.NewPublicScope(ctx, d, cfg.StorageAPIHost, dependencies.WithLogIndexLoading(true))
	if err != nil {
		return nil, err
	}

	d.EtcdClientScope, err = dependencies.NewEtcdClientScope(ctx, d, cfg.Etcd)
	if err != nil {
		return nil, err
	}

	d.TaskScope, err = dependencies.NewTaskScope(ctx, cfg.NodeID, d)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func newServiceScope(parentScp parentScopes) ServiceScope {
	d := &serviceScope{}

	d.parentScopes = parentScp

	d.hookRegistry, d.hookExecutor = hook.New()

	d.definitionRepository = definitionRepo.New(d)

	return d
}

func (v *serviceScope) HookRegistry() *hook.Registry {
	return v.hookRegistry
}

func (v *serviceScope) HookExecutor() *hook.Executor {
	return v.hookExecutor
}

func (v *serviceScope) DefinitionRepository() *definitionRepo.Repository {
	return v.definitionRepository
}
