// Package dependencies provides dependencies for Templates API.
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
//   - Container [ForServer] is created in API main.go entrypoint, in "start" method, see [src/github.com/keboola/keboola-as-code/cmd/templates-api/main.go].
//   - Container [ForPublicRequest] is created for each HTTP request in the http.ContextMiddleware function, see [src/github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/http/middleware.go].
//   - Container [ForProjectRequest] is created for each authenticated HTTP request in the service.APIKeyAuth method, see [src/github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/service/auth.go].
//
// Dependencies injection to service endpoints:
//   - Each service endpoint handler/method gets [ForPublicRequest] container as a parameter.
//   - If the endpoint use token authentication it gets [ForProjectRequest] container instead.
//   - It is ensured by [src/github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/dependencies] package.
//   - See service implementation for details [src/github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/service/service.go].
package dependencies

import (
	"context"
	"fmt"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManager "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type ctxKey string

const (
	ForPublicRequestCtxKey       = ctxKey("ForPublicRequest")
	ForProjectRequestCtxKey      = ctxKey("ForProjectRequest")
	EtcdConnectionTimeoutCtxKey  = ctxKey("EtcdConnectionTimeout")
	EtcdDefaultConnectionTimeout = 2 * time.Second
	ProjectLockTTLSeconds        = 60
)

// ForServer interface provides dependencies for Templates API server.
// The container exists during the entire run of the API server.
type ForServer interface {
	dependencies.Base
	dependencies.Public
	Process() *servicectx.Process
	RepositoryManager() *repositoryManager.Manager
	EtcdClient(ctx context.Context) (*etcd.Client, error)
	ProjectLocker() *Locker
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
	Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error)
	TemplateRepository(ctx context.Context, reference model.TemplateRepository) (*repository.Repository, error)
	ProjectRepositories() *model.TemplateRepositories
}

// forServer implements ForServer interface.
type forServer struct {
	dependencies.Base
	dependencies.Public
	proc              *servicectx.Process
	debug             bool
	logger            log.Logger
	repositoryManager *repositoryManager.Manager
	etcdClient        dependencies.Lazy[*etcd.Client]
	projectLocker     dependencies.Lazy[*Locker]
}

// forPublicRequest implements ForPublicRequest interface.
type forPublicRequest struct {
	ForServer
	logger     log.Logger
	requestCtx context.Context
	requestID  string
	components dependencies.Lazy[*model.ComponentsMap]
}

// forProjectRequest implements ForProjectRequest interface.
type forProjectRequest struct {
	dependencies.Project
	ForPublicRequest
	logger              log.Logger
	repositories        map[string]*repositoryManager.CachedRepository
	projectRepositories dependencies.Lazy[*model.TemplateRepositories]
}

func NewServerDeps(ctx context.Context, proc *servicectx.Process, envs env.Provider, logger log.Logger, defaultRepositories []model.TemplateRepository, debug, dumpHTTP bool) (v ForServer, err error) {
	// Create tracer
	var tracer trace.Tracer = nil
	if telemetry.IsDataDogEnabled(envs) {
		tracer = telemetry.NewDataDogTracer()
		_, span := tracer.Start(ctx, "kac.lib.api.server.templates.dependencies.NewServerDeps")
		defer telemetry.EndSpan(span, &err)
	}

	// Get Storage API host
	storageAPIHost := strhelper.NormalizeHost(envs.MustGet("KBC_STORAGE_API_HOST"))
	if storageAPIHost == "" {
		return nil, errors.New("KBC_STORAGE_API_HOST environment variable is not set")
	}

	// Create base HTTP client for all API requests to other APIs
	httpClient := httpclient.New(
		httpclient.WithUserAgent("keboola-templates-api"),
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
	// Create base dependencies
	baseDeps := dependencies.NewBaseDeps(envs, tracer, logger, httpClient)

	// Create public dependencies - load API index
	startTime := time.Now()
	logger.Info("loading Storage API index")
	publicDeps, err := dependencies.NewPublicDeps(ctx, baseDeps, storageAPIHost, true)
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded Storage API index | %s", time.Since(startTime))

	// Create server dependencies
	d := &forServer{
		Base:   baseDeps,
		Public: publicDeps,
		proc:   proc,
		debug:  debug,
		logger: logger,
	}

	// Create repository manager
	if v, err := repositoryManager.New(ctx, d, defaultRepositories); err != nil {
		return nil, err
	} else {
		d.repositoryManager = v
	}

	// Test connection to etcd at server startup
	// We use a longer timeout when starting the server, because ETCD could be restarted at the same time as the API.
	etcdCtx := context.WithValue(ctx, EtcdConnectionTimeoutCtxKey, 30*time.Second)
	if _, err := d.EtcdClient(etcdCtx); err != nil {
		d.Logger().Warnf(err.Error())
	}

	return d, nil
}

func NewDepsForPublicRequest(serverDeps ForServer, requestCtx context.Context, requestId string) ForPublicRequest {
	_, span := serverDeps.Tracer().Start(requestCtx, "kac.api.server.templates.dependencies.NewDepsForPublicRequest")
	defer telemetry.EndSpan(span, nil)

	return &forPublicRequest{
		ForServer:  serverDeps,
		logger:     serverDeps.Logger().AddPrefix(fmt.Sprintf("[requestId=%s]", requestId)),
		requestCtx: requestCtx,
		requestID:  requestId,
	}
}

func NewDepsForProjectRequest(publicDeps ForPublicRequest, ctx context.Context, tokenStr string) (ForProjectRequest, error) {
	ctx, span := publicDeps.Tracer().Start(ctx, "kac.api.server.templates.dependencies.NewDepsForProjectRequest")
	defer telemetry.EndSpan(span, nil)

	projectDeps, err := dependencies.NewProjectDeps(ctx, publicDeps, publicDeps, tokenStr)
	if err != nil {
		return nil, err
	}

	logger := publicDeps.Logger().AddPrefix(
		fmt.Sprintf("[project=%d][token=%s]", projectDeps.ProjectID(), projectDeps.StorageAPITokenID()),
	)

	return &forProjectRequest{
		logger:           logger,
		Project:          projectDeps,
		ForPublicRequest: publicDeps,
		repositories:     make(map[string]*repositoryManager.CachedRepository),
	}, nil
}

func (v *forServer) Process() *servicectx.Process {
	return v.proc
}

func (v *forServer) RepositoryManager() *repositoryManager.Manager {
	return v.repositoryManager
}

func (v *forServer) EtcdClient(ctx context.Context) (*etcd.Client, error) {
	return v.etcdClient.InitAndGet(func() (*etcd.Client, error) {
		ctx, span := v.Tracer().Start(ctx, "kac.api.server.templates.dependencies.EtcdClient")
		defer telemetry.EndSpan(span, nil)

		// Get timeout
		// We use a longer timeout on the API start,
		// the etcd could be started at the same time as the API.
		connectTimeout := EtcdDefaultConnectionTimeout
		if v, found := ctx.Value(EtcdConnectionTimeoutCtxKey).(time.Duration); found {
			connectTimeout = v
		}

		envs := v.Envs()
		if envs.Get("TEMPLATES_API_ETCD_ENABLED") == "false" {
			return nil, errors.New("etcd integration is disabled")
		}

		etcdClient, err := etcdclient.New(
			// Use separated context.
			// Graceful shutdown is handled by Process.OnShutdown bellow.
			// During the shutdown it is necessary to complete some etcd operations.
			context.Background(),
			v.Tracer(),
			envs.Get("TEMPLATES_API_ETCD_ENDPOINT"),
			envs.Get("TEMPLATES_API_ETCD_NAMESPACE"),
			etcdclient.WithUsername(envs.Get("TEMPLATES_API_ETCD_USERNAME")),
			etcdclient.WithPassword(envs.Get("TEMPLATES_API_ETCD_PASSWORD")),
			etcdclient.WithConnectContext(ctx),
			etcdclient.WithConnectTimeout(connectTimeout),
			etcdclient.WithLogger(v.logger),
			etcdclient.WithDebugOpLogs(v.debug),
		)
		if err != nil {
			return nil, err
		}

		// Close client when shutting down the server
		v.proc.OnShutdown(func() {
			if err := etcdClient.Close(); err != nil {
				v.logger.Warnf("cannot close etcd connection: %s", err)
			} else {
				v.logger.Info("closed etcd connection")
			}
		})

		return etcdClient, nil
	})
}

func (v *forServer) ProjectLocker() *Locker {
	return v.projectLocker.MustInitAndGet(func() *Locker {
		return NewLocker(v, ProjectLockTTLSeconds)
	})
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

func (v *forPublicRequest) Components() *model.ComponentsMap {
	// Use the same version of the components during the entire request
	return v.components.MustInitAndGet(func() *model.ComponentsMap {
		return v.ForServer.Components()
	})
}

func (v *forProjectRequest) Logger() log.Logger {
	return v.logger
}

func (v *forProjectRequest) ProjectRepositories() *model.TemplateRepositories {
	return v.projectRepositories.MustInitAndGet(func() *model.TemplateRepositories {
		// Project repositories are default repositories modified by the project features.
		features := v.ProjectFeatures()
		out := model.NewTemplateRepositories()
		for _, repo := range v.RepositoryManager().DefaultRepositories() {
			if repo.Name == repository.DefaultTemplateRepositoryName && repo.Ref == repository.DefaultTemplateRepositoryRefMain {
				if features.Has(repository.FeatureTemplateRepositoryBeta) {
					repo.Ref = repository.DefaultTemplateRepositoryRefBeta
				} else if features.Has(repository.FeatureTemplateRepositoryDev) {
					repo.Ref = repository.DefaultTemplateRepositoryRefDev
				}
			}
			out.Add(repo)
		}
		return out
	})
}

func (v *forProjectRequest) Template(ctx context.Context, reference model.TemplateRef) (tmpl *template.Template, err error) {
	ctx, span := v.Tracer().Start(ctx, "kac.api.server.templates.dependencies.Template")
	defer telemetry.EndSpan(span, &err)

	// Get repository
	repo, err := v.cachedTemplateRepository(ctx, reference.Repository())
	if err != nil {
		return nil, err
	}

	// Get template
	return repo.Template(ctx, reference)
}

func (v *forProjectRequest) TemplateRepository(ctx context.Context, definition model.TemplateRepository) (tmpl *repository.Repository, err error) {
	ctx, span := v.Tracer().Start(ctx, "kac.api.server.templates.dependencies.TemplateRepository")
	defer telemetry.EndSpan(span, &err)

	repo, err := v.cachedTemplateRepository(ctx, definition)
	if err != nil {
		return nil, err
	}
	return repo.Unwrap(), nil
}

func (v *forProjectRequest) cachedTemplateRepository(ctx context.Context, definition model.TemplateRepository) (repo *repositoryManager.CachedRepository, err error) {
	if _, found := v.repositories[definition.Hash()]; !found {
		ctx, span := v.Tracer().Start(ctx, "kac.api.server.templates.dependencies.cachedTemplateRepository")
		defer telemetry.EndSpan(span, &err)

		// Get git repository
		repo, unlockFn, err := v.RepositoryManager().Repository(ctx, definition)
		if err != nil {
			return nil, err
		}

		// Unlock repository after the request,
		// so repository directory won't be deleted during request (if a new version has been pulled).
		go func() {
			<-v.RequestCtx().Done()
			unlockFn()
		}()

		// Cache value for the request
		v.repositories[definition.Hash()] = repo
	}

	return v.repositories[definition.Hash()], nil
}
