// Package dependencies provides common dependencies for Buffer API / Worker.
package dependencies

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const DistributionWorkerGroupName = "buffer-worker"

type ForService interface {
	dependencies.Base
	dependencies.Public
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Schema() *schema.Schema
	Store() *store.Store
	StatsCache() *statistics.CacheNode
}

func NewServiceDeps(
	ctx context.Context,
	proc *servicectx.Process,
	cfg config.Config,
	logger log.Logger,
	tel telemetry.Telemetry,
	userAgent string,
) (d ForService, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.dependencies.NewServiceDeps")
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

	// Create base dependencies
	baseDeps := dependencies.NewBaseDeps(envs, logger, tel, httpClient)

	// Create public dependencies - load API index
	publicDeps, err := dependencies.NewPublicDeps(
		ctx, baseDeps, cfg.StorageAPIHost,
		dependencies.WithLogIndexLoading(true),
	)
	if err != nil {
		return nil, err
	}

	// Create etcd client
	etcdClient, err := etcdclient.New(
		ctx,
		proc,
		tel,
		cfg.EtcdEndpoint,
		cfg.EtcdNamespace,
		etcdclient.WithUsername(cfg.EtcdUsername),
		etcdclient.WithPassword(cfg.EtcdPassword),
		etcdclient.WithConnectTimeout(cfg.EtcdConnectTimeout),
		etcdclient.WithLogger(logger),
		etcdclient.WithDebugOpLogs(cfg.DebugEtcd),
	)
	if err != nil {
		return nil, err
	}

	serviceDeps := &forService{
		Base:       baseDeps,
		Public:     publicDeps,
		proc:       proc,
		etcdClient: etcdClient,
		etcdSerde:  serde.NewJSON(baseDeps.Validator().Validate),
		schema:     schema.New(baseDeps.Validator().Validate),
	}

	serviceDeps.store = store.New(serviceDeps)

	serviceDeps.statsCache, err = statistics.NewCacheNode(serviceDeps)
	if err != nil {
		return nil, err
	}

	return serviceDeps, nil
}

// forServer implements ForService interface.
type forService struct {
	dependencies.Base
	dependencies.Public
	proc       *servicectx.Process
	etcdClient *etcd.Client
	etcdSerde  *serde.Serde
	schema     *schema.Schema
	store      *store.Store
	statsCache *statistics.CacheNode
}

func (v *forService) Process() *servicectx.Process {
	return v.proc
}

func (v *forService) EtcdClient() *etcd.Client {
	return v.etcdClient
}

func (v *forService) EtcdSerde() *serde.Serde {
	return v.etcdSerde
}

func (v *forService) Schema() *schema.Schema {
	return v.schema
}

func (v *forService) Store() *store.Store {
	return v.store
}

func (v *forService) StatsCache() *statistics.CacheNode {
	return v.statsCache
}
