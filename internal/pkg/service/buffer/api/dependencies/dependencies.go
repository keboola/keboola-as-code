// Package dependencies provides dependencies for Buffer API.
//
// # Dependency Containers
//
// This package extends common dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies].
//
// These dependencies containers are implemented:
//   - [ForServer] long-lived dependencies that exist during the entire run of the API server.
//   - [ForPublicRequest] short-lived dependencies for a public request without authentication.
//   - [ForProjectRequest] short-lived dependencies for a request with authentication.
//
// Dependency containers creation:
//   - Container [ForServer] is created in API main.go entrypoint, in "start" method, see [src/github.com/keboola/keboola-as-code/cmd/buffer-api/main.go].
//   - Container [ForPublicRequest] is created for each HTTP request in the http.ContextMiddleware function, see [src/github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/http/middleware.go].
//   - Container [ForProjectRequest] is created for each authenticated HTTP request in the service.APIKeyAuth method, see [src/github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/auth.go].
//
// Dependencies injection to service endpoints:
//   - Each service endpoint handler/method gets [ForPublicRequest] container as a parameter.
//   - If the endpoint use token authentication it gets [ForProjectRequest] container instead.
//   - It is ensured by [src/github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/dependencies] package.
//   - See service implementation for details [src/github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/service.go].
package dependencies

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ctxKey string

const (
	ForPublicRequestCtxKey       = ctxKey("ForPublicRequest")
	ForProjectRequestCtxKey      = ctxKey("ForProjectRequest")
	EtcdConnectionTimeoutCtxKey  = ctxKey("EtcdConnectionTimeout")
	EtcdDefaultConnectionTimeout = 2 * time.Second
	EtcdKeepAliveTimeout         = 2 * time.Second
	EtcdKeepAliveInterval        = 10 * time.Second
	ProjectLockTTLSeconds        = 60
)

// ForServer interface provides dependencies for Buffer API server.
// The container exists during the entire run of the API server.
type ForServer interface {
	dependencies.Base
	dependencies.Public
	ServerCtx() context.Context
	ServerWaitGroup() *sync.WaitGroup
	PrefixLogger() log.PrefixLogger
	EtcdClient() *etcd.Client
	ConfigStore() *ConfigStore
	BufferApiHost() string
}

// ForPublicRequest interface provides dependencies for a public request that does not contain the Storage API token.
// The container exists only during request processing.
type ForPublicRequest interface {
	ForServer
	RequestCtx() context.Context
	RequestID() string
}

// ForProjectRequest interface provides dependencies for an authenticated request that contains the Storage API token.
// The container exists only during request processing.
type ForProjectRequest interface {
	ForPublicRequest
	dependencies.Project
}

// forServer implements ForServer interface.
type forServer struct {
	dependencies.Base
	dependencies.Public
	serverCtx     context.Context
	serverWg      *sync.WaitGroup
	logger        log.PrefixLogger
	etcdClient    *etcd.Client
	configStore   *ConfigStore
	bufferApiHost string
}

// forPublicRequest implements ForPublicRequest interface.
type forPublicRequest struct {
	ForServer
	logger     log.PrefixLogger
	requestCtx context.Context
	requestID  string
}

// forProjectRequest implements ForProjectRequest interface.
type forProjectRequest struct {
	dependencies.Project
	ForPublicRequest
	logger log.PrefixLogger
}

func NewServerDeps(serverCtx context.Context, envs env.Provider, logger log.PrefixLogger, debug, dumpHttp bool) (v ForServer, err error) {
	// Create tracer
	ctx := serverCtx
	var tracer trace.Tracer = nil
	if telemetry.IsDataDogEnabled(envs) {
		var span trace.Span
		tracer = telemetry.NewDataDogTracer()
		ctx, span = tracer.Start(ctx, "kac.lib.api.server.buffer.dependencies.NewServerDeps")
		defer telemetry.EndSpan(span, &err)
	} else {
		tracer = telemetry.NewNopTracer()
	}

	// Create wait group - for graceful shutdown
	serverWg := &sync.WaitGroup{}

	// Get Storage API host
	storageApiHost := strhelper.NormalizeHost(envs.Get("KBC_STORAGE_API_HOST"))
	if storageApiHost == "" {
		return nil, errors.New("KBC_STORAGE_API_HOST environment variable is empty or not set")
	}

	// Get Buffer API host
	bufferApiHost := strhelper.NormalizeHost(envs.Get("KBC_BUFFER_API_HOST"))
	if bufferApiHost == "" {
		return nil, errors.New("KBC_BUFFER_API_HOST environment variable is empty or not set")
	}

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		httpclient.WithUserAgent("keboola-buffer-api"),
		httpclient.WithEnvs(envs),
		func(c *httpclient.Config) {
			if debug {
				httpclient.WithDebugOutput(logger.DebugWriter())(c)
			}
			if dumpHttp {
				httpclient.WithDumpOutput(logger.DebugWriter())(c)
			}
		},
	)

	// Create base dependencies
	baseDeps := dependencies.NewBaseDeps(envs, tracer, logger, httpClient)

	// Create public dependencies - load API index
	startTime := time.Now()
	logger.Info("loading Storage API index")
	publicDeps, err := dependencies.NewPublicDeps(serverCtx, baseDeps, storageApiHost)
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded Storage API index | %s", time.Since(startTime))

	// Connect to ETCD
	// We use a longer timeout when starting the server, because ETCD could be restarted at the same time as the API.
	etcdClient, err := etcdclient.New(
		serverCtx,
		tracer,
		envs.Get("BUFFER_ETCD_ENDPOINT"),
		envs.Get("BUFFER_ETCD_NAMESPACE"),
		etcdclient.WithUsername(envs.Get("BUFFER_ETCD_USERNAME")),
		etcdclient.WithPassword(envs.Get("BUFFER_ETCD_PASSWORD")),
		etcdclient.WithConnectContext(ctx),
		etcdclient.WithConnectTimeout(30*time.Second), // longer timeout, the etcd could be started at the same time as the API
		etcdclient.WithLogger(logger),
		etcdclient.WithWaitGroup(serverWg),
	)
	if err != nil {
		return nil, err
	}

	// Create config store
	configStore := NewConfigStore(logger, etcdClient, validator.New(), tracer)

	// Create server dependencies
	d := &forServer{
		Base:          baseDeps,
		Public:        publicDeps,
		serverCtx:     serverCtx,
		serverWg:      serverWg,
		logger:        logger,
		etcdClient:    etcdClient,
		configStore:   configStore,
		bufferApiHost: bufferApiHost,
	}

	return d, nil
}

func NewDepsForPublicRequest(serverDeps ForServer, requestCtx context.Context, requestId string) ForPublicRequest {
	_, span := serverDeps.Tracer().Start(requestCtx, "kac.api.server.buffer.dependencies.NewDepsForPublicRequest")
	defer telemetry.EndSpan(span, nil)

	return &forPublicRequest{
		ForServer:  serverDeps,
		logger:     serverDeps.PrefixLogger().WithAdditionalPrefix(fmt.Sprintf("[requestId=%s]", requestId)),
		requestCtx: requestCtx,
		requestID:  requestId,
	}
}

func NewDepsForProjectRequest(publicDeps ForPublicRequest, ctx context.Context, tokenStr string) (ForProjectRequest, error) {
	ctx, span := publicDeps.Tracer().Start(ctx, "kac.api.server.buffer.dependencies.NewDepsForProjectRequest")
	defer telemetry.EndSpan(span, nil)

	projectDeps, err := dependencies.NewProjectDeps(ctx, publicDeps, publicDeps, tokenStr)
	if err != nil {
		return nil, err
	}

	logger := publicDeps.PrefixLogger().WithAdditionalPrefix(
		fmt.Sprintf("[project=%d][token=%s]", projectDeps.ProjectID(), projectDeps.StorageApiTokenID()),
	)

	return &forProjectRequest{
		logger:           logger,
		Project:          projectDeps,
		ForPublicRequest: publicDeps,
	}, nil
}

func (v *forServer) ServerCtx() context.Context {
	return v.serverCtx
}

func (v *forServer) ServerWaitGroup() *sync.WaitGroup {
	return v.serverWg
}

func (v *forServer) PrefixLogger() log.PrefixLogger {
	return v.logger
}

func (v *forServer) EtcdClient() *etcd.Client {
	return v.etcdClient
}

func (v *forServer) ConfigStore() *ConfigStore {
	return v.configStore
}

func (v *forServer) BufferApiHost() string {
	return v.bufferApiHost
}

func (v *forPublicRequest) Logger() log.Logger {
	return v.logger
}

func (v *forPublicRequest) PrefixLogger() log.PrefixLogger {
	return v.logger
}

func (v *forPublicRequest) RequestCtx() context.Context {
	return v.requestCtx
}

func (v *forPublicRequest) RequestID() string {
	return v.requestID
}

func (v *forProjectRequest) Logger() log.Logger {
	return v.logger
}

func (v *forProjectRequest) PrefixLogger() log.PrefixLogger {
	return v.logger
}
