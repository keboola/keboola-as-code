// Package dependencies provides common dependencies for Buffer API / Worker.
package dependencies

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ForService interface {
	dependencies.Base
	dependencies.Public
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	Store() *store.Store
}

func NewServiceDeps(
	ctx context.Context,
	proc *servicectx.Process,
	tracer trace.Tracer,
	envs env.Provider,
	logger log.Logger,
	debug, dumpHTTP bool,
	userAgent string,
) (d ForService, err error) {
	ctx, span := tracer.Start(ctx, "keboola.go.buffer.dependencies.NewServiceDeps")
	defer telemetry.EndSpan(span, &err)

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		httpclient.WithUserAgent(userAgent),
		httpclient.WithEnvs(envs),
		func(c *httpclient.Config) {
			if debug {
				httpclient.WithDebugOutput(logger.DebugWriter())(c)
			}
			if dumpHTTP {
				httpclient.WithDumpOutput(logger.DebugWriter())(c)
			}
		},
	)

	// Get Storage API host
	storageAPIHost := strhelper.NormalizeHost(envs.Get("KBC_STORAGE_API_HOST"))
	if storageAPIHost == "" {
		return nil, errors.New("KBC_STORAGE_API_HOST environment variable is empty or not set")
	}

	// Create base dependencies
	baseDeps := dependencies.NewBaseDeps(envs, tracer, logger, httpClient)

	// Create public dependencies - load API index
	startTime := time.Now()
	logger.Info("loading Storage API index")
	publicDeps, err := dependencies.NewPublicDeps(ctx, baseDeps, storageAPIHost)
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded Storage API index | %s", time.Since(startTime))

	// Create etcd client
	etcdClient, err := etcdclient.New(
		// Use separated context.
		// Graceful shutdown is handled by Process.OnShutdown bellow.
		// During the shutdown it is necessary to complete some etcd operations.
		context.Background(),
		tracer,
		envs.Get("BUFFER_ETCD_ENDPOINT"),
		envs.Get("BUFFER_ETCD_NAMESPACE"),
		etcdclient.WithUsername(envs.Get("BUFFER_ETCD_USERNAME")),
		etcdclient.WithPassword(envs.Get("BUFFER_ETCD_PASSWORD")),
		etcdclient.WithConnectContext(ctx),
		etcdclient.WithConnectTimeout(30*time.Second), // longer timeout, the etcd could be started at the same time as the API/Worker
		etcdclient.WithLogger(logger),
		etcdclient.WithDebugOpLogs(debug),
	)
	if err != nil {
		return nil, err
	}

	// Close client when shutting down the server
	proc.OnShutdown(func() {
		if err := etcdClient.Close(); err != nil {
			logger.Warnf("cannot close etcd connection: %s", err)
		} else {
			logger.Info("closed etcd connection")
		}
	})

	schemaInst := schema.New(validator.New().Validate)
	storeInst := store.New(logger, etcdClient, tracer, schemaInst, clock.New())

	return &forService{
		Base:       baseDeps,
		Public:     publicDeps,
		proc:       proc,
		etcdClient: etcdClient,
		schema:     schemaInst,
		store:      storeInst,
	}, nil
}

// forServer implements ForService interface.
type forService struct {
	dependencies.Base
	dependencies.Public
	proc       *servicectx.Process
	etcdClient *etcd.Client
	schema     *schema.Schema
	store      *store.Store
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
