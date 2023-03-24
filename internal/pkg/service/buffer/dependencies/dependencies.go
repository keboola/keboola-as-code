// Package dependencies provides common dependencies for Buffer API / Worker.
package dependencies

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ForService interface {
	dependencies.Base
	dependencies.Public
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	Store() *store.Store
	StatsCacheNode() *statistics.CacheNode
}

func NewServiceDeps(
	ctx context.Context,
	proc *servicectx.Process,
	tracer trace.Tracer,
	cfg config.Config,
	envs env.Provider,
	logger log.Logger,
	userAgent string,
) (d ForService, err error) {
	ctx, span := tracer.Start(ctx, "keboola.go.buffer.dependencies.NewServiceDeps")
	defer telemetry.EndSpan(span, &err)

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		httpclient.WithUserAgent(userAgent),
		httpclient.WithEnvs(envs),
		func(c *httpclient.Config) {
			if cfg.Debug {
				httpclient.WithDebugOutput(logger.DebugWriter())(c)
			}
			if cfg.DebugHTTP {
				httpclient.WithDumpOutput(logger.DebugWriter())(c)
			}
		},
	)

	// Create base dependencies
	baseDeps := dependencies.NewBaseDeps(envs, tracer, logger, httpClient)

	// Create public dependencies - load API index
	startTime := time.Now()
	logger.Info("loading Storage API index")
	publicDeps, err := dependencies.NewPublicDeps(ctx, baseDeps, cfg.StorageAPIHost)
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded Storage API index | %s", time.Since(startTime))

	// Create etcd client
	etcdClient, err := etcdclient.New(
		ctx,
		proc,
		tracer,
		cfg.EtcdEndpoint,
		cfg.EtcdNamespace,
		etcdclient.WithUsername(cfg.EtcdUsername),
		etcdclient.WithPassword(cfg.EtcdPassword),
		etcdclient.WithConnectTimeout(cfg.EtcdConnectTimeout),
		etcdclient.WithLogger(logger),
		etcdclient.WithDebugOpLogs(cfg.Debug),
	)
	if err != nil {
		return nil, err
	}

	serviceDeps := &forService{
		Base:       baseDeps,
		Public:     publicDeps,
		proc:       proc,
		etcdClient: etcdClient,
		schema:     schema.New(validator.New().Validate),
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

func (v *forService) Schema() *schema.Schema {
	return v.schema
}

func (v *forService) Store() *store.Store {
	return v.store
}

func (v *forService) StatsCacheNode() *statistics.CacheNode {
	return v.statsCache
}
