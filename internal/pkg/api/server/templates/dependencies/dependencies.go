package dependencies

import (
	"context"
	stdLog "log"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type ctxKey string

const CtxKey = ctxKey("dependencies")

// Container provides dependencies used only in the API + common dependencies.
type Container interface {
	dependencies.Common
	Ctx() context.Context
	CtxCancelFn() context.CancelFunc
	WithCtx(ctx context.Context) Container
	PrefixLogger() log.PrefixLogger
	RepositoryManager() (*repository.Manager, error)
	WithLoggerPrefix(prefix string) *container
	WithStorageApi(api *storageapi.Api) (*container, error)
}

// NewContainer returns dependencies for API and add them to the context.
func NewContainer(ctx context.Context, debug bool, logger *stdLog.Logger, envs *env.Map) Container {
	ctx, cancel := context.WithCancel(ctx)
	c := &container{ctx: ctx, ctxCancelFn: cancel, debug: debug, envs: envs, logger: log.NewApiLogger(logger, "", debug)}
	c.commonDeps = dependencies.NewCommonContainer(c)
	return c
}

type commonDeps = dependencies.Common

type container struct {
	commonDeps
	ctx               context.Context
	ctxCancelFn       context.CancelFunc
	debug             bool
	logger            log.PrefixLogger
	envs              *env.Map
	repositoryManager *repository.Manager
	storageApi        *storageapi.Api
}

func (v *container) Ctx() context.Context {
	return v.ctx
}

func (v *container) CtxCancelFn() context.CancelFunc {
	return v.ctxCancelFn
}

func (v *container) WithCtx(ctx context.Context) Container {
	clone := *v
	clone.ctx = ctx
	return &clone
}

// WithLoggerPrefix returns dependencies clone with modified logger.
func (v *container) WithLoggerPrefix(prefix string) *container {
	clone := *v
	clone.logger = v.logger.WithPrefix(prefix)
	return &clone
}

// WithStorageApi returns dependencies clone with modified Storage API.
func (v *container) WithStorageApi(api *storageapi.Api) (*container, error) {
	clone := *v
	clone.storageApi = api
	return &clone, nil
}

func (v *container) Logger() log.Logger {
	return v.logger
}

func (v *container) PrefixLogger() log.PrefixLogger {
	return v.logger
}

func (v *container) RepositoryManager() (*repository.Manager, error) {
	if v.repositoryManager == nil {
		if manager, err := repository.NewManager(v.Logger()); err != nil {
			return nil, err
		} else {
			v.repositoryManager = manager
		}
	}
	return v.repositoryManager, nil
}

func (v *container) Envs() *env.Map {
	return v.envs
}

func (v *container) ApiVerboseLogs() bool {
	return v.debug
}

func (v *container) StorageApi() (*storageapi.Api, error) {
	// Store API instance, so it can be cloned, see WithStorageApi
	if v.storageApi == nil {
		api, err := v.commonDeps.StorageApi()
		if err != nil {
			return nil, err
		}
		v.storageApi = api
	}

	return v.storageApi, nil
}

func (v *container) StorageApiHost() (string, error) {
	return strhelper.NormalizeHost(v.envs.MustGet("KBC_STORAGE_API_HOST")), nil
}

func (v *container) StorageApiToken() (string, error) {
	// The API is authorized separately in each request
	return "", nil
}
