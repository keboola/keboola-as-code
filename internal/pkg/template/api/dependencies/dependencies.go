package dependencies

import (
	"context"
	"fmt"
	stdLog "log"

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
}

func (v *container) CtxCancelFn() context.CancelFunc {
	return v.ctxCancelFn
}

// WithLoggerPrefix returns dependencies clone with modified logger.
func (v *container) WithLoggerPrefix(prefix string) (*container, error) {
	clone := *v
	clone.logger = v.logger.WithPrefix(prefix)
	return &clone, nil
}

func (v *container) Logger() log.Logger {
	return v.logger
}

func (v *container) Envs() *env.Map {
	return v.envs
}

func (v *container) ApiVerboseLogs() bool {
	return v.debug
}

func (v *container) StorageApiHost() (string, error) {
	panic(fmt.Errorf("not implemented yet"))
}

func (v *container) StorageApiToken() (string, error) {
	panic(fmt.Errorf("not implemented yet"))
}
