package dependencies

import (
	"context"
	"io"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	aggregationRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/aggregation/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	keboolaSinkBridge "github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	userAgent = "keboola-stream"
)

// serviceScope implements ServiceScope interface.
type serviceScope struct {
	dependencies.BaseScope
	dependencies.PublicScope
	dependencies.EtcdClientScope
	dependencies.TaskScope
	plugins                     *plugin.Plugins
	definitionRepository        *definitionRepo.Repository
	storageRepository           *storageRepo.Repository
	storageStatisticsRepository *statsRepo.Repository
	aggregationRepository       *aggregationRepo.Repository
	keboolaBridge               *keboolaSinkBridge.Bridge
}

type parentScopes struct {
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
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.stream.dependencies.NewServiceScope")
	defer span.End(&err)

	p, err := newParentScopes(ctx, cfg, proc, logger, tel, stdout, stderr)
	if err != nil {
		return nil, err
	}

	return newServiceScope(p.BaseScope, p.PublicScope, p.EtcdClientScope, cfg, model.DefaultBackoff())
}

func newParentScopes(
	ctx context.Context,
	cfg config.Config,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v *parentScopes, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.stream.dependencies.newParentScopes")
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

	d := &parentScopes{}

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

func NewMockedServiceScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	tb.Helper()
	return NewMockedServiceScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedServiceScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	tb.Helper()

	// Create common mocked dependencies
	commonMock := dependencies.NewMocked(tb, ctx, append(
		[]dependencies.MockedOption{
			dependencies.WithEnabledEtcdClient(),
			dependencies.WithMockedStorageAPIHost("connection.keboola.local"),
		},
		opts...,
	)...)

	// Get and modify test config
	cfg := testConfig(tb, commonMock)
	if modifyConfig != nil {
		modifyConfig(&cfg)
	}

	// Create service mocked dependencies
	mock := &mocked{Mocked: commonMock, config: cfg, dummySinkController: dummy.NewController()}

	backoff := model.NoRandomizationBackoff()
	serviceScp, err := newServiceScope(mock, mock, mock, cfg, backoff)
	require.NoError(tb, err)

	mock.DebugLogger().Truncate()
	// TODO: reset only when mock transport exists
	// mock.MockedHTTPTransport().Reset()

	mock.dummySinkController.RegisterDummySinkTypes(serviceScp.Plugins(), mock.TestDummySinkController())

	return serviceScp, mock
}

func newServiceScope(baseScp dependencies.BaseScope, publicScp dependencies.PublicScope, etcdClientScp dependencies.EtcdClientScope, cfg config.Config, storageBackoff model.RetryBackoff) (ServiceScope, error) {
	var err error

	d := &serviceScope{}

	d.BaseScope = baseScp

	d.PublicScope = publicScp

	d.EtcdClientScope = etcdClientScp

	d.plugins = plugin.New(d.Logger())

	d.definitionRepository = definitionRepo.New(d)

	d.storageRepository, err = storageRepo.New(cfg.Storage.Level, d, storageBackoff)
	if err != nil {
		return nil, err
	}

	d.plugins.RegisterSinkWithLocalStorage(func(sinkType definition.SinkType) bool {
		return sinkType == definition.SinkTypeTable
	})

	apiCtxProvider := func(ctx context.Context) *keboola.AuthorizedAPI {
		api, _ := ctx.Value(KeboolaProjectAPICtxKey).(*keboola.AuthorizedAPI)
		return api
	}

	d.keboolaBridge = keboolaSinkBridge.New(d, apiCtxProvider)

	d.storageStatisticsRepository = statsRepo.New(d)

	d.aggregationRepository = aggregationRepo.New(d)

	return d, nil
}

func (v *serviceScope) Plugins() *plugin.Plugins {
	return v.plugins
}

func (v *serviceScope) DefinitionRepository() *definitionRepo.Repository {
	return v.definitionRepository
}

func (v *serviceScope) KeboolaSinkBridge() *keboolaSinkBridge.Bridge {
	return v.keboolaBridge
}

func (v *serviceScope) StorageRepository() *storageRepo.Repository {
	return v.storageRepository
}

func (v *serviceScope) StatisticsRepository() *statsRepo.Repository {
	return v.storageStatisticsRepository
}

func (v *serviceScope) AggregationRepository() *aggregationRepo.Repository {
	return v.aggregationRepository
}
