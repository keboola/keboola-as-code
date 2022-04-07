package dependencies

import (
	"context"
	stdLog "log"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type ctxKey string

const CtxKey = ctxKey("dependencies")

// Container provides dependencies used only in the API + common dependencies.
type Container interface {
	dependencies.Common
	CtxCancelFn() context.CancelFunc
	PrefixLogger() log.PrefixLogger
	LoggerPrefix() string
	WithLoggerPrefix(prefix string) *container
	WithStorageApi(api *storageapi.Api) (*container, error)
}

// NewContainer returns dependencies for API and add them to the context.
func NewContainer(ctx context.Context, debug bool, logger *stdLog.Logger, envs *env.Map) Container {
	ctx, cancel := context.WithCancel(ctx)
	c := &container{ctxCancelFn: cancel, debug: debug, envs: envs, logger: log.NewApiLogger(logger, "", debug)}
	c.commonDeps = dependencies.NewCommonContainer(c, ctx)
	return c
}

type commonDeps = dependencies.Common

type container struct {
	commonDeps
	ctxCancelFn context.CancelFunc
	debug       bool
	logger      log.PrefixLogger
	envs        *env.Map
	storageApi  *storageapi.Api
}

func (v *container) CtxCancelFn() context.CancelFunc {
	return v.ctxCancelFn
}

func (v *container) LoggerPrefix() string {
	return v.logger.Prefix()
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
	return v.envs.MustGet("KBC_STORAGE_API_HOST"), nil
}

func (v *container) StorageApiToken() (string, error) {
	// The API is authorized separately in each request
	return "", nil
}
