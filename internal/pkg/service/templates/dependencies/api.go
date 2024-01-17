package dependencies

import (
	"context"
	"io"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	repositoryManager "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
)

const (
	userAgent             = "keboola-templates-api"
	distributionGroupName = "templates-api"
)

// apiScope implements APIScope interface.
type apiScope struct {
	parentScopes
	config            config.Config
	schema            *schema.Schema
	store             *store.Store
	repositoryManager *repositoryManager.Manager
	projectLocker     *Locker
}

type parentScopes interface {
	dependencies.BaseScope
	dependencies.PublicScope
	dependencies.EtcdClientScope
	dependencies.TaskScope
	dependencies.DistributionScope
}

type parentScopesImpl struct {
	dependencies.BaseScope
	dependencies.PublicScope
	dependencies.EtcdClientScope
	dependencies.TaskScope
	dependencies.DistributionScope
}

func NewAPIScope(
	ctx context.Context,
	cfg config.Config,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v APIScope, err error) {
	parentSc, err := newParentScopes(ctx, cfg, proc, logger, tel, stdout, stderr)
	if err != nil {
		return nil, err
	}
	return newAPIScope(ctx, parentSc, cfg)
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
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.templates.api.dependencies.newParentScopes")
	defer span.End(&err)

	httpClient := httpclient.New(
		httpclient.WithTelemetry(tel),
		httpclient.WithUserAgent(userAgent),
		func(c *httpclient.Config) {
			if cfg.DebugLog {
				httpclient.WithDebugOutput(stdout)(c)
			}
			if cfg.DebugHTTP {
				httpclient.WithDumpOutput(stdout)(c)
			}
		},
	)

	d := &parentScopesImpl{}

	d.BaseScope = dependencies.NewBaseScope(ctx, logger, tel, stdout, stderr, clock.New(), proc, httpClient)

	d.PublicScope, err = dependencies.NewPublicScope(
		ctx, d, cfg.StorageAPIHost,
		dependencies.WithLogIndexLoading(true),
		dependencies.WithPreloadComponents(true),
	)
	if err != nil {
		return nil, err
	}

	d.EtcdClientScope, err = dependencies.NewEtcdClientScope(
		ctx, d, cfg.Etcd,
		etcdclient.WithConnectTimeout(cfg.EtcdConnectTimeout),
		etcdclient.WithDebugOpLogs(cfg.DebugEtcd),
	)
	if err != nil {
		return nil, err
	}

	d.TaskScope, err = dependencies.NewTaskScope(ctx, d)
	if err != nil {
		return nil, err
	}

	d.DistributionScope, err = dependencies.NewDistributionScope(ctx, d, distributionGroupName)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func newAPIScope(ctx context.Context, parentScp parentScopes, cfg config.Config) (v *apiScope, err error) {
	ctx, span := parentScp.Telemetry().Tracer().Start(ctx, "keboola.go.templates.api.dependencies.NewAPIScope")
	defer span.End(&err)

	d := &apiScope{}

	d.parentScopes = parentScp

	d.config = cfg

	d.schema = schema.New(d)

	d.store = store.New(d)

	d.repositoryManager, err = repositoryManager.New(ctx, d, cfg.Repositories)
	if err != nil {
		return nil, err
	}

	d.projectLocker = NewLocker(d, ProjectLockTTLSeconds)

	return d, nil
}

func (v *apiScope) APIConfig() config.Config {
	return v.config
}

func (v *apiScope) Schema() *schema.Schema {
	return v.schema
}

func (v *apiScope) Store() *store.Store {
	return v.store
}

func (v *apiScope) RepositoryManager() *repositoryManager.Manager {
	return v.repositoryManager
}

func (v *apiScope) ProjectLocker() *Locker {
	return v.projectLocker
}
