package dependencies

import (
	"context"
	"fmt"
	stdLog "log"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	etcd "go.etcd.io/etcd/client/v3"

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
const EtcdTestConnectionTimeout = 1 * time.Second

// Container provides dependencies used only in the API + common dependencies.
type Container interface {
	dependencies.Common
	Ctx() context.Context
	CtxCancelFn() context.CancelFunc
	WithCtx(ctx context.Context, cancelFn context.CancelFunc) Container
	PrefixLogger() log.PrefixLogger
	RepositoryManager() (*repository.Manager, error)
	ProjectRepositories() ([]model.TemplateRepository, error)
	TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error)
	WithLoggerPrefix(prefix string) *container
	WithStorageApiClient(client client.Client, token *storageapi.Token) (*container, error)
	EtcdClient() (*etcd.Client, error)
	Locker() *Locker
}

// NewContainer returns dependencies for API and add them to the context.
func NewContainer(ctx context.Context, repositories []model.TemplateRepository, debug, debugHttp bool, logger *stdLog.Logger, envs *env.Map) (Container, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &container{ctx: ctx, ctxCancelFn: cancel, debug: debug, debugHttp: debugHttp, envs: envs, logger: log.NewApiLogger(logger, "", debug)}
	c.commonDeps = dependencies.NewCommonContainer(ctx, c)

	// Create repository manager
	if m, err := repository.NewManager(ctx, c.logger, repositories); err != nil {
		return nil, err
	} else {
		c.repositoryManager = m
	}

	return c, nil
}

type commonDeps = dependencies.CommonContainer

type container struct {
	*commonDeps
	ctx               context.Context
	ctxCancelFn       context.CancelFunc
	httpClient        *client.Client
	debug             bool // enables debug log level
	debugHttp         bool // log HTTP client request and response bodies
	logger            log.PrefixLogger
	envs              *env.Map
	repositoryManager *repository.Manager
	etcdClient        dependencies.Lazy[*etcd.Client]
	locker            dependencies.Lazy[*Locker]
}

func (v *container) Ctx() context.Context {
	return v.ctx
}

func (v *container) CtxCancelFn() context.CancelFunc {
	return v.ctxCancelFn
}

func (v *container) ProjectRepositories() ([]model.TemplateRepository, error) {
	// Currently, all projects use the same repositories, in the future it may differ per project.
	features, err := v.ProjectFeatures()
	if err != nil {
		return nil, err
	}

	var out []model.TemplateRepository
	for _, repo := range v.repositoryManager.DefaultRepositories() {
		if repo.Name == repository.DefaultTemplateRepositoryName && repo.Ref == repository.DefaultTemplateRepositoryRefMain {
			if features.Has(repository.FeatureTemplateRepositoryBeta) {
				repo.Ref = repository.DefaultTemplateRepositoryRefBeta
			} else if features.Has(repository.FeatureTemplateRepositoryDev) {
				repo.Ref = repository.DefaultTemplateRepositoryRefDev
			}
		}
		out = append(out, repo)
	}
	return out, nil
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

func (v *container) EtcdClient() (*etcd.Client, error) {
	return v.etcdClient.InitAndGet(func() (**etcd.Client, error) {
		// Check if etcd is enabled
		if v.envs.Get("ETCD_ENABLED") == "false" {
			return nil, fmt.Errorf("etcd integration is disabled")
		}

		// Get endpoint
		endpoint := v.envs.Get("ETCD_ENDPOINT")
		if endpoint == "" {
			return nil, fmt.Errorf("ETCD_HOST is not set")
		}

		// Create client
		c, err := etcd.New(etcd.Config{
			Context:              v.ctx,
			Endpoints:            []string{endpoint},
			DialTimeout:          2 * time.Second,
			DialKeepAliveTimeout: 2 * time.Second,
			DialKeepAliveTime:    10 * time.Second,
			Username:             v.envs.Get("ETCD_USERNAME"), // optional
			Password:             v.envs.Get("ETCD_PASSWORD"), // optional
		})
		if err != nil {
			return nil, err
		}

		// Sync endpoints list from cluster (also serves as a connection check)
		syncCtx, syncCancelFn := context.WithTimeout(v.ctx, EtcdTestConnectionTimeout)
		defer syncCancelFn()
		if err := c.Sync(syncCtx); err != nil {
			c.Close()
			return nil, err
		}

		// Close client when shutting down the server
		go func() {
			<-v.ctx.Done()
			c.Close()
		}()

		return &c, nil
	})
}

func (v *container) Locker() *Locker {
	locker, _ := v.locker.InitAndGet(func() (**Locker, error) {
		locker := &Locker{d: v}
		return &locker, nil
	})
	return locker
}
