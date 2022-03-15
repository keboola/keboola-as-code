package dependencies

import (
	"context"
	"fmt"
	stdLog "log"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// Container provides dependencies used only in the API + common dependencies.
type Container interface {
	dependencies.Common
}

func NewContainer(ctx context.Context, debug bool, logger *stdLog.Logger, envs *env.Map) Container {
	c := &container{debug: debug, envs: envs, logger: log.NewApiLogger(logger, "", debug)}
	c.commonDeps = dependencies.NewCommonContainer(c, ctx)
	return c
}

type commonDeps = dependencies.Common

type container struct {
	commonDeps
	debug  bool
	logger log.PrefixLogger
	envs   *env.Map
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
