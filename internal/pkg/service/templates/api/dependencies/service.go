package dependencies

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ForService interface {
	dependencies.Base
	dependencies.Public
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Schema() *schema.Schema
	Store() *store.Store
	TaskNode() *task.Node
}

func NewServiceDeps(
	ctx context.Context,
	proc *servicectx.Process,
	cfg config.Config,
	envs env.Provider,
	logger log.Logger,
	tel telemetry.Telemetry,
	userAgent string,
) (d ForService, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.templates.dependencies.NewServiceDeps")
	defer telemetry.EndSpan(span, &err)

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		tel,
		httpclient.WithUserAgent(userAgent),
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
	baseDeps := dependencies.NewBaseDeps(envs, logger, tel, httpClient)

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
		tel,
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

	validateFn := validator.New().Validate
	serviceDeps := &forService{
		Base:       baseDeps,
		Public:     publicDeps,
		proc:       proc,
		etcdClient: etcdClient,
		etcdSerde:  serde.NewJSON(validateFn),
		schema:     schema.New(validateFn),
	}
	serviceDeps.store = store.New(serviceDeps)
	serviceDeps.taskNode, err = task.NewNode(serviceDeps, task.WithSpanNamePrefix(config.SpanNamePrefix))
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
	taskNode   *task.Node
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

func (v *forService) TaskNode() *task.Node {
	return v.taskNode
}
