package dependencies

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// serviceScope implements ServiceScope interface.
type serviceScope struct {
	parentScopes
	config          config.ServiceConfig
	schema          *schema.Schema
	store           *store.Store
	fileManager     *file.Manager
	statsRepository *statistics.Repository
	statsL1Cache    *statistics.L1CacheProvider
	statsL2Cache    *statistics.L2CacheProvider
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

func (v *serviceScope) ServiceConfig() config.ServiceConfig {
	return v.config
}

func (v *serviceScope) Schema() *schema.Schema {
	return v.schema
}

func (v *serviceScope) Store() *store.Store {
	return v.store
}

func (v *serviceScope) FileManager() *file.Manager {
	return v.fileManager
}

func (v *serviceScope) StatisticsRepository() *statistics.Repository {
	return v.statsRepository
}

func (v *serviceScope) StatisticsL1Cache() *statistics.L1CacheProvider {
	return v.statsL1Cache
}

func (v *serviceScope) StatisticsL2Cache() *statistics.L2CacheProvider {
	return v.statsL2Cache
}

func NewServiceScope(ctx context.Context, cfg config.ServiceConfig, proc *servicectx.Process, logger log.Logger, tel telemetry.Telemetry, userAgent string) (v ServiceScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.dependencies.NewServiceScope")
	defer span.End(&err)
	parentSc, err := newParentScopes(ctx, cfg, proc, logger, tel, userAgent)
	if err != nil {
		return nil, err
	}
	return newServiceScope(parentSc, cfg)
}

func newParentScopes(ctx context.Context, cfg config.ServiceConfig, proc *servicectx.Process, logger log.Logger, tel telemetry.Telemetry, userAgent string) (v parentScopes, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.dependencies.newParentScopes")
	defer span.End(&err)

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		httpclient.WithTelemetry(tel),
		httpclient.WithUserAgent(userAgent),
		func(c *httpclient.Config) {
			if cfg.DebugLog {
				httpclient.WithDebugOutput(logger.DebugWriter())(c)
			}
			if cfg.DebugHTTP {
				httpclient.WithDumpOutput(logger.DebugWriter())(c)
			}
		},
	)

	d := &parentScopesImpl{}

	d.BaseScope = dependencies.NewBaseScope(ctx, logger, tel, clock.New(), proc, httpClient)

	d.PublicScope, err = dependencies.NewPublicScope(ctx, d, cfg.StorageAPIHost, dependencies.WithLogIndexLoading(true))
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

	d.TaskScope, err = dependencies.NewTaskScope(ctx, cfg.NodeID, d)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func newServiceScope(parentScp parentScopes, cfg config.ServiceConfig) (v ServiceScope, err error) {
	d := &serviceScope{}

	d.config = cfg

	d.parentScopes = parentScp

	d.schema = schema.New(d.Validator().Validate)

	d.statsL1Cache, err = statistics.NewL1CacheProvider(d)
	if err != nil {
		return nil, err
	}

	d.statsL2Cache, err = statistics.NewL2CacheProvider(d.statsL1Cache, d)
	if err != nil {
		return nil, err
	}

	d.statsRepository = statistics.NewRepository(statistics.NewAtomicProvider(d), d)

	d.store = store.New(d)

	d.fileManager = file.NewManager(d)

	return d, nil
}
