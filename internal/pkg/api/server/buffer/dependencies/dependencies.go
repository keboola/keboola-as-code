// Package dependencies provides dependencies for Buffer API.
//
// # Dependency Containers
//
// This package extends common dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/dependencies].
//
// These dependencies containers are implemented:
//   - [ForServer] long-lived dependencies that exist during the entire run of the API server.
//   - [ForPublicRequest] short-lived dependencies for a public request without authentication.
//   - [ForProjectRequest] short-lived dependencies for a request with authentication.
//
// Dependency containers creation:
//   - Container [ForServer] is created in API main.go entrypoint, in "start" method, see [src/github.com/keboola/keboola-as-code/cmd/buffer-api/main.go].
//   - Container [ForPublicRequest] is created for each HTTP request in the http.ContextMiddleware function, see [src/github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/http/middleware.go].
//   - Container [ForProjectRequest] is created for each authenticated HTTP request in the service.APIKeyAuth method, see [src/github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/service/auth.go].
//
// Dependencies injection to service endpoints:
//   - Each service endpoint handler/method gets [ForPublicRequest] container as a parameter.
//   - If the endpoint use token authentication it gets [ForProjectRequest] container instead.
//   - It is ensured by [src/github.com/keboola/keboola-as-code/internal/pkg/api/server/common/extension/dependencies] package.
//   - See service implementation for details [src/github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/service/service.go].
package dependencies

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/client"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"
	ddHttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/common/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	telemetryUtils "github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
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
	EtcdClient(ctx context.Context) (*etcd.Client, error)
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
	serverCtx  context.Context
	serverWg   *sync.WaitGroup
	logger     log.PrefixLogger
	etcdClient dependencies.Lazy[*etcd.Client]
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
	var tracer trace.Tracer = nil
	if telemetry.IsDataDogEnabled(envs) {
		tracer = telemetry.NewDataDogTracer()
		_, span := tracer.Start(serverCtx, "kac.lib.api.server.buffer.dependencies.NewServerDeps")
		defer telemetryUtils.EndSpan(span, &err)
	}

	// Create wait group - for graceful shutdown
	serverWg := &sync.WaitGroup{}

	// Get Storage API host
	storageApiHost := strhelper.NormalizeHost(envs.MustGet("KBC_STORAGE_API_HOST"))
	if storageApiHost == "" {
		return nil, errors.New("KBC_STORAGE_API_HOST environment variable is not set")
	}

	// Create base HTTP client for all API requests to other APIs
	httpClient := apiHttpClient(envs, logger, debug, dumpHttp)

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

	// Create server dependencies
	d := &forServer{
		Base:      baseDeps,
		Public:    publicDeps,
		serverCtx: serverCtx,
		serverWg:  serverWg,
		logger:    logger,
	}

	// Test connection to etcd at server startup
	// We use a longer timeout when starting the server, because ETCD could be restarted at the same time as the API.
	etcdCtx := context.WithValue(serverCtx, EtcdConnectionTimeoutCtxKey, 30*time.Second)
	if _, err := d.EtcdClient(etcdCtx); err != nil {
		d.Logger().Warnf("cannot connect to etcd: %s", err.Error())
	}

	return d, nil
}

func NewDepsForPublicRequest(serverDeps ForServer, requestCtx context.Context, requestId string) ForPublicRequest {
	_, span := serverDeps.Tracer().Start(requestCtx, "kac.api.server.buffer.dependencies.NewDepsForPublicRequest")
	defer telemetryUtils.EndSpan(span, nil)

	return &forPublicRequest{
		ForServer:  serverDeps,
		logger:     serverDeps.PrefixLogger().WithAdditionalPrefix(fmt.Sprintf("[requestId=%s]", requestId)),
		requestCtx: requestCtx,
		requestID:  requestId,
	}
}

func NewDepsForProjectRequest(publicDeps ForPublicRequest, ctx context.Context, tokenStr string) (ForProjectRequest, error) {
	ctx, span := publicDeps.Tracer().Start(ctx, "kac.api.server.buffer.dependencies.NewDepsForProjectRequest")
	defer telemetryUtils.EndSpan(span, nil)

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

func (v *forServer) EtcdClient(ctx context.Context) (*etcd.Client, error) {
	return v.etcdClient.InitAndGet(func() (*etcd.Client, error) {
		ctx, span := v.Tracer().Start(ctx, "kac.api.server.buffer.dependencies.EtcdClient")
		defer telemetryUtils.EndSpan(span, nil)

		// Check if etcd is enabled
		if v.Envs().Get("ETCD_ENABLED") == "false" {
			return nil, errors.New("etcd integration is disabled")
		}

		// Get endpoint
		endpoint := v.Envs().Get("ETCD_ENDPOINT")
		if endpoint == "" {
			return nil, errors.New("ETCD_HOST is not set")
		}

		// Get timeout
		connectTimeout := EtcdDefaultConnectionTimeout
		if v, found := ctx.Value(EtcdConnectionTimeoutCtxKey).(time.Duration); found {
			connectTimeout = v
		}

		// Create client
		startTime := time.Now()
		v.logger.Infof("connecting to etcd, timeout=%s", connectTimeout)
		c, err := etcd.New(etcd.Config{
			Context:              v.serverCtx, // !!! a long-lived context must be used, client exists as long as the entire server
			Endpoints:            []string{endpoint},
			DialTimeout:          connectTimeout,
			DialKeepAliveTimeout: EtcdKeepAliveTimeout,
			DialKeepAliveTime:    EtcdKeepAliveInterval,
			Username:             v.Envs().Get("ETCD_USERNAME"), // optional
			Password:             v.Envs().Get("ETCD_PASSWORD"), // optional
		})
		if err != nil {
			return nil, err
		}

		// Context for connection test
		syncCtx, syncCancelFn := context.WithTimeout(ctx, connectTimeout)
		defer syncCancelFn()

		// Sync endpoints list from cluster (also serves as a connection check)
		if err := c.Sync(syncCtx); err != nil {
			c.Close()
			return nil, err
		}

		// Close client when shutting down the server
		wg := v.ServerWaitGroup()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-v.serverCtx.Done()
			if err := c.Close(); err != nil {
				v.Logger().Warnf("cannot close connection etcd: %s", err)
			} else {
				v.Logger().Info("closed connection to etcd")
			}
		}()

		v.logger.Infof(`connected to etcd cluster "%s" | %s`, c.Endpoints()[0], time.Since(startTime))
		return c, nil
	})
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

func apiHttpClient(envs env.Provider, logger log.Logger, debug, dumpHttp bool) client.Client {
	// Force HTTP2 transport
	transport := client.HTTP2Transport()

	// DataDog low-level tracing (raw http requests)
	if telemetry.IsDataDogEnabled(envs) {
		transport = ddHttp.WrapRoundTripper(
			transport,
			ddHttp.WithBefore(func(request *http.Request, span ddtrace.Span) {
				// We use "http.request" operation name for request to the API,
				// so requests to other API must have different operation name.
				span.SetOperationName("kac.api.client.http.request")
				span.SetTag("http.host", request.URL.Host)
				if dotPos := strings.IndexByte(request.URL.Host, '.'); dotPos > 0 {
					// E.g. connection, encryption, scheduler ...
					span.SetTag("http.hostPrefix", request.URL.Host[:dotPos])
				}
				span.SetTag(ext.EventSampleRate, 1.0)
				span.SetTag(ext.HTTPURL, request.URL.Redacted())
				span.SetTag("http.path", request.URL.Path)
				span.SetTag("http.query", request.URL.Query().Encode())
			}),
			ddHttp.RTWithResourceNamer(func(r *http.Request) string {
				// Set resource name to request path
				return strhelper.MustUrlPathUnescape(r.URL.RequestURI())
			}))
	}

	// Create client
	c := client.New().
		WithTransport(transport).
		WithUserAgent("keboola-buffer-api")

	// Log each HTTP client request/response as debug message
	if debug {
		c = c.AndTrace(client.LogTracer(logger.DebugWriter()))
	}

	// Dump each HTTP client request/response body
	if dumpHttp {
		c = c.AndTrace(client.DumpTracer(logger.DebugWriter()))
	}

	// DataDog high-level tracing (api client requests)
	if telemetry.IsDataDogEnabled(envs) {
		c = c.AndTrace(telemetry.ApiClientTrace())
	}

	return c
}
