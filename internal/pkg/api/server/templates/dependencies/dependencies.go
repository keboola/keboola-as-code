package dependencies

import (
	"context"
	stdLog "log"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

type ctxKey string

const CtxKey = ctxKey("dependencies")

// Container provides dependencies used only in the API + common dependencies.
type Container interface {
	dependencies.Common
	Ctx() context.Context
	CtxCancelFn() context.CancelFunc
	WithCtx(ctx context.Context, cancelFn context.CancelFunc) Container
	PrefixLogger() log.PrefixLogger
	RepositoryManager() (*repository.Manager, error)
	Repositories() ([]model.TemplateRepository, error)
	TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error)
	WithLoggerPrefix(prefix string) *container
	WithStorageApiClient(client client.Client, token *storageapi.Token) (*container, error)
}

// NewContainer returns dependencies for API and add them to the context.
func NewContainer(ctx context.Context, repositories []model.TemplateRepository, debug, debugHttp bool, logger *stdLog.Logger, envs *env.Map) (Container, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &container{ctx: ctx, ctxCancelFn: cancel, defaultRepositories: repositories, debug: debug, debugHttp: debugHttp, envs: envs, logger: log.NewApiLogger(logger, "", debug)}
	c.commonDeps = dependencies.NewCommonContainer(ctx, c)
	return c, nil
}

type commonDeps = dependencies.CommonContainer

type container struct {
	*commonDeps
	ctx                 context.Context
	ctxCancelFn         context.CancelFunc
	httpClient          *client.Client
	debug               bool // enables debug log level
	debugHttp           bool // log HTTP client request and response bodies
	logger              log.PrefixLogger
	envs                *env.Map
	repositoryManager   *repository.Manager
	defaultRepositories []model.TemplateRepository
}

func (v *container) Ctx() context.Context {
	return v.ctx
}

func (v *container) CtxCancelFn() context.CancelFunc {
	return v.ctxCancelFn
}

func (v *container) Repositories() ([]model.TemplateRepository, error) {
	// Currently, all projects use the same repositories, in the future it may differ per project.
	features, err := v.Features()
	if err != nil {
		return nil, err
	}

	var out []model.TemplateRepository
	for _, repo := range v.defaultRepositories {
		if repo.Name == "keboola" && repo.Ref == "main" {
			if features.Has("feature-abc") {
				repo.Ref = "abc"
			} else if features.Has("feature-def") {
				repo.Ref = "def"
			}
		}
		out = append(out, repo)
	}
	return out
}
func (v *container) WithCtx(ctx context.Context, cancelFn context.CancelFunc) Container {
	clone := *v
	clone.ctx = ctx
	if cancelFn != nil {
		clone.ctxCancelFn = cancelFn
	}
	return &clone
}

// WithLoggerPrefix returns dependencies clone with modified logger.
func (v *container) WithLoggerPrefix(prefix string) *container {
	clone := *v
	clone.logger = v.logger.WithAdditionalPrefix(prefix)
	return &clone
}

func (v *container) HttpClient() client.Client {
	if v.httpClient == nil {
		// Force HTTP2 transport
		transport := client.HTTP2Transport()

		// DataDog low-level tracing
		if v.envs.Get("DATADOG_ENABLED") != "false" {
			transport = http.WrapRoundTripper(transport)
		}

		// Create client
		c := client.New().
			WithTransport(transport).
			WithUserAgent("keboola-templates-api")

		// Log each HTTP client request/response as debug message
		if v.debug {
			c = c.AndTrace(client.LogTracer(v.logger.DebugWriter()))
		}

		// Dump each HTTP client request/response body
		if v.debugHttp {
			c = c.AndTrace(client.DumpTracer(v.logger.DebugWriter()))
		}

		// DataDog high-level tracing
		if v.envs.Get("DATADOG_ENABLED") != "false" {
			c = c.AndTrace(DDApiClientTrace())
		}
		v.httpClient = &c
	}
	return *v.httpClient
}

// WithStorageApiClient returns dependencies clone with modified Storage API.
func (v *container) WithStorageApiClient(client client.Client, token *storageapi.Token) (*container, error) {
	clone := *v
	clone.commonDeps = clone.commonDeps.WithStorageApiClient(client, token)
	return &clone, nil
}

func (v *container) Logger() log.Logger {
	return v.logger
}

func (v *container) PrefixLogger() log.PrefixLogger {
	return v.logger
}

func (v *container) Components() (*model.ComponentsMap, error) {
	// Get components provider
	provider, err := v.commonDeps.ComponentsProvider()
	if err != nil {
		return nil, err
	}

	// Acquire read lock and release it after request,
	// so update cannot occur in the middle of the request.
	provider.RLock()
	go func() {
		<-v.ctx.Done()
		provider.RUnlock()
	}()
	return provider.Components(), nil
}

func (v *container) RepositoryManager() (*repository.Manager, error) {
	if v.repositoryManager == nil {
		// Register default repositories
		v.repositoryManager = repository.NewManager(v.Ctx(), v.Logger())
		for _, repo := range v.defaultRepositories {
			if repo.Type == model.RepositoryTypeGit {
				if err := v.repositoryManager.AddRepository(repo); err != nil {
					return nil, err
				}
			}
		}
	}
	return v.repositoryManager, nil
}

func (v *container) TemplateRepository(definition model.TemplateRepository, _ model.TemplateRef) (*repository.Repository, error) {
	var fs filesystem.Fs
	var err error
	if definition.Type == model.RepositoryTypeGit {
		// Get manager
		manager, err := v.RepositoryManager()
		if err != nil {
			return nil, err
		}

		// Get git repository
		gitRepository, err := manager.Repository(definition)
		if err != nil {
			return nil, err
		}

		// Acquire read lock and release it after request,
		// so pull cannot occur in the middle of the request.
		gitRepository.RLock()
		go func() {
			<-v.ctx.Done()
			gitRepository.RUnlock()
		}()
		fs = gitRepository.Fs()
	} else {
		fs, err = aferofs.NewLocalFs(v.logger, definition.Url, ".")
		if err != nil {
			return nil, err
		}
	}

	// Load manifest from FS
	manifest, err := loadRepositoryManifest.Run(fs, v)
	if err != nil {
		return nil, err
	}

	// Return repository
	return repository.New(definition, fs, manifest)
}

func (v *container) Envs() *env.Map {
	return v.envs
}

func (v *container) StorageApiHost() (string, error) {
	return strhelper.NormalizeHost(v.envs.MustGet("KBC_STORAGE_API_HOST")), nil
}

func (v *container) StorageApiToken() (string, error) {
	// The API is authorized separately in each request
	return "", nil
}
