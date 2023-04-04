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

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/config"
	serviceDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/token"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ip"
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
	APIConfig() config.Config
	StatsCollector() *statistics.CollectorNode
	WatcherAPINode() *watcher.APINode
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
	TokenManager() *token.Manager
	TableManager() *table.Manager
	TaskNode() *task.Node
	FileManager() *file.Manager
}

// forServer implements ForServer interface.
type forServer struct {
	serviceDependencies.ForService
	config  config.Config
	stats   *statistics.CollectorNode
	watcher *watcher.APINode
}

// forPublicRequest implements ForPublicRequest interface.
type forPublicRequest struct {
	ForServer
	logger     log.Logger
	request    *http.Request
	requestCtx context.Context
	requestID  string
}

// forProjectRequest implements ForProjectRequest interface.
type forProjectRequest struct {
	dependencies.Project
	ForPublicRequest
	logger       log.Logger
	tokenManager *token.Manager
	tableManager *table.Manager
	taskNode     *task.Node
	fileManager  *file.Manager
}

func NewServerDeps(ctx context.Context, proc *servicectx.Process, cfg config.Config, envs env.Provider, logger log.Logger) (v ForServer, err error) {
	// Create tracer
	var tracer trace.Tracer = nil
	if telemetry.IsDataDogEnabled(envs) {
		var span trace.Span
		tracer = telemetry.NewDataDogTracer()
		ctx, span = tracer.Start(ctx, "keboola.go.buffer.api.dependencies.NewServerDeps")
		defer telemetry.EndSpan(span, &err)
	} else {
		tracer = telemetry.NewNopTracer()
	}

	// Create service dependencies
	userAgent := "keboola-buffer-api"
	serviceDeps, err := serviceDependencies.NewServiceDeps(ctx, proc, tracer, cfg.ServiceConfig, envs, logger, userAgent)
	if err != nil {
		return nil, err
	}

	// Create server dependencies
	d := &forServer{
		ForService: serviceDeps,
		config:     cfg,
	}

	d.stats = statistics.NewCollectorNode(d)

	d.watcher, err = watcher.NewAPINode(d)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func NewDepsForPublicRequest(serverDeps ForServer, requestCtx context.Context, requestId string, request *http.Request) ForPublicRequest {
	_, span := serverDeps.Tracer().Start(requestCtx, "kac.api.server.buffer.dependencies.NewDepsForPublicRequest")
	defer telemetry.EndSpan(span, nil)

	return &forPublicRequest{
		ForServer:  serverDeps,
		logger:     serverDeps.Logger().AddPrefix(fmt.Sprintf("[requestId=%s]", requestId)),
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

	logger := publicDeps.Logger().AddPrefix(
		fmt.Sprintf("[project=%d][token=%s]", projectDeps.ProjectID(), projectDeps.StorageAPITokenID()),
	)

	d := &forProjectRequest{
		Project:          projectDeps,
		ForPublicRequest: publicDeps,
		logger:           logger,
	}
	d.tokenManager = token.NewManager(d)
	d.tableManager = table.NewManager(d.KeboolaProjectAPI())
	d.fileManager = file.NewManager(d.Clock(), d.KeboolaProjectAPI(), nil)
	d.taskNode, err = task.NewNode(d)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (v *forServer) APIConfig() config.Config {
	return v.config
}

func (v *forServer) StatsCollector() *statistics.CollectorNode {
	return v.stats
}

func (v *forServer) WatcherAPINode() *watcher.APINode {
	return v.watcher
}

func (v *forPublicRequest) Logger() log.Logger {
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
	return ip.From(v.request)
}

func (v *forProjectRequest) Logger() log.Logger {
	return v.logger
}

func (v *forProjectRequest) TokenManager() *token.Manager {
	return v.tokenManager
}

func (v *forProjectRequest) TableManager() *table.Manager {
	return v.tableManager
}

func (v *forProjectRequest) TaskNode() *task.Node {
	return v.taskNode
}

func (v *forProjectRequest) FileManager() *file.Manager {
	return v.fileManager
}

func (v *forProjectRequest) KeboolaProjectAPI() *keboola.API {
	return v.Project.KeboolaProjectAPI()
}
