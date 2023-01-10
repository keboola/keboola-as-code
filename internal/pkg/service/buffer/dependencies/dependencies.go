// Package dependencies provides common dependencies for Buffer API / Worker.
package dependencies

import (
	"context"
	"time"

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
		ctx,
		proc,
		tracer,
		envs.Get("BUFFER_ETCD_ENDPOINT"),
		envs.Get("BUFFER_ETCD_NAMESPACE"),
		etcdclient.WithUsername(envs.Get("BUFFER_ETCD_USERNAME")),
		etcdclient.WithPassword(envs.Get("BUFFER_ETCD_PASSWORD")),
		etcdclient.WithConnectTimeout(30*time.Second), // longer timeout, the etcd could be started at the same time as the API/Worker
		etcdclient.WithLogger(logger),
		etcdclient.WithDebugOpLogs(debug),
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
