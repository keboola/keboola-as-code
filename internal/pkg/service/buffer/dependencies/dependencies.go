// Package dependencies provides common dependencies for Buffer API / Worker.
package dependencies

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/configstore"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/recordstore"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/runtimestore"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ForService interface {
	dependencies.Base
	dependencies.Public
	ConfigStore() *configstore.Store
	RecordStore() *recordstore.Store
	RuntimeStore() *runtimestore.Store
}

func NewServiceDeps(
	processCtx, ctx context.Context,
	processWg *sync.WaitGroup,
	tracer trace.Tracer,
	envs env.Provider,
	logger log.PrefixLogger,
	debug, dumpHTTP bool,
	userAgent string,
) (d ForService, err error) {
	ctx, span := tracer.Start(ctx, "keboola.go.buffer.dependencies.NewServiceDeps")
	defer telemetry.EndSpan(span, &err)

	// Create validator
	validatorInst := validator.New()

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
	publicDeps, err := dependencies.NewPublicDeps(processCtx, baseDeps, storageAPIHost)
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded Storage API index | %s", time.Since(startTime))

	// Create etcd client
	etcdClient, err := etcdclient.New(
		processCtx,
		tracer,
		envs.Get("BUFFER_ETCD_ENDPOINT"),
		envs.Get("BUFFER_ETCD_NAMESPACE"),
		etcdclient.WithUsername(envs.Get("BUFFER_ETCD_USERNAME")),
		etcdclient.WithPassword(envs.Get("BUFFER_ETCD_PASSWORD")),
		etcdclient.WithConnectContext(ctx),
		etcdclient.WithConnectTimeout(30*time.Second), // longer timeout, the etcd could be started at the same time as the API/Worker
		etcdclient.WithLogger(logger),
		etcdclient.WithDebugOpLogs(debug),
		etcdclient.WithWaitGroup(processWg),
	)
	if err != nil {
		return nil, err
	}

	return &forService{
		Base:         baseDeps,
		Public:       publicDeps,
		configStore:  configstore.New(logger, etcdClient, validatorInst, tracer),
		recordStore:  recordstore.New(logger, etcdClient, validatorInst, tracer),
		runtimeStore: runtimestore.New(logger, etcdClient, validatorInst, tracer),
	}, nil
}

// forServer implements ForService interface.
type forService struct {
	dependencies.Base
	dependencies.Public
	configStore  *configstore.Store
	recordStore  *recordstore.Store
	runtimeStore *runtimestore.Store
}

func (v *forService) ConfigStore() *configstore.Store {
	return v.configStore
}

func (v *forService) RecordStore() *recordstore.Store {
	return v.recordStore
}

func (v *forService) RuntimeStore() *runtimestore.Store {
	return v.runtimeStore
}
