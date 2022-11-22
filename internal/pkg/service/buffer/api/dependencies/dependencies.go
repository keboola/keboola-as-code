// Package dependencies provides dependencies for Buffer API.
//
// # Dependency Containers
//
// This package extends:
//   - common dependencies from  [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies].
//   - service dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies].
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
	"net"
	"net/http"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	serviceDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type ctxKey string

const (
	ForPublicRequestCtxKey  = ctxKey("ForPublicRequest")
	ForProjectRequestCtxKey = ctxKey("ForProjectRequest")
)

// ForServer interface provides dependencies for Buffer API server.
// The container exists during the entire run of the API server.
type ForServer interface {
	serviceDependencies.ForService
	ServerCtx() context.Context
	ServerWaitGroup() *sync.WaitGroup
	PrefixLogger() log.PrefixLogger
	BufferApiHost() string
}

// ForPublicRequest interface provides dependencies for a public request that does not contain the Storage API token.
// The container exists only during request processing.
type ForPublicRequest interface {
	ForServer
	RequestCtx() context.Context
	RequestID() string
	RequestHeader() http.Header
	RequestClientIP() net.IP
}

// ForProjectRequest interface provides dependencies for an authenticated request that contains the Storage API token.
// The container exists only during request processing.
type ForProjectRequest interface {
	ForPublicRequest
	dependencies.Project
}

// forServer implements ForServer interface.
type forServer struct {
	serviceDependencies.ForService
	serverCtx     context.Context
	serverWg      *sync.WaitGroup
	logger        log.PrefixLogger
	bufferApiHost string
}

// forPublicRequest implements ForPublicRequest interface.
type forPublicRequest struct {
	ForServer
	logger     log.PrefixLogger
	request    *http.Request
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
		ctx, span = tracer.Start(ctx, "keboola.go.buffer.api.dependencies.NewServerDeps")
		defer telemetry.EndSpan(span, &err)
	} else {
		tracer = telemetry.NewNopTracer()
	}

	// Create wait group - for graceful shutdown
	serverWg := &sync.WaitGroup{}

	// Get Buffer API host
	bufferApiHost := strhelper.NormalizeHost(envs.Get("KBC_BUFFER_API_HOST"))
	if bufferApiHost == "" {
		return nil, errors.New("KBC_BUFFER_API_HOST environment variable is empty or not set")
	}

	// Create service dependencies
	userAgent := "keboola-buffer-api"
	serviceDeps, err := serviceDependencies.NewServiceDeps(serverCtx, ctx, serverWg, tracer, envs, logger, debug, dumpHttp, userAgent)
	if err != nil {
		return nil, err
	}

	// Create server dependencies
	d := &forServer{
		ForService:    serviceDeps,
		serverCtx:     serverCtx,
		serverWg:      serverWg,
		logger:        logger,
		bufferApiHost: bufferApiHost,
	}

	return d, nil
}

func NewDepsForPublicRequest(serverDeps ForServer, requestCtx context.Context, requestId string, request *http.Request) ForPublicRequest {
	_, span := serverDeps.Tracer().Start(requestCtx, "kac.api.server.buffer.dependencies.NewDepsForPublicRequest")
	defer telemetry.EndSpan(span, nil)

	return &forPublicRequest{
		ForServer:  serverDeps,
		logger:     serverDeps.PrefixLogger().WithAdditionalPrefix(fmt.Sprintf("[requestId=%s]", requestId)),
		request:    request,
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

func (v *forPublicRequest) RequestHeader() http.Header {
	return v.request.Header.Clone()
}

func (v *forPublicRequest) RequestClientIP() net.IP {
	// Get IP from the X-REAL-IP header
	ip := v.request.Header.Get("X-REAL-IP")
	if netIP := net.ParseIP(ip); netIP != nil {
		return netIP
	}

	// Get IP from X-FORWARDED-FOR header
	ips := v.request.Header.Get("X-FORWARDED-FOR")
	splitIps := strings.Split(ips, ",")
	for _, ip := range splitIps {
		if netIP := net.ParseIP(ip); netIP != nil {
			return netIP
		}
	}

	// Get IP from RemoteAddr
	ip, _, _ = net.SplitHostPort(v.request.RemoteAddr)
	if netIP := net.ParseIP(ip); netIP != nil {
		return netIP
	}

	return nil
}

func (v *forProjectRequest) Logger() log.Logger {
	return v.logger
}

func (v *forProjectRequest) PrefixLogger() log.PrefixLogger {
	return v.logger
}
