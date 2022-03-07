package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// Container provides dependencies used only in the CLI + common dependencies.
type Container interface {
	dependencies.Common
}

func NewContainer(ctx context.Context, envs *env.Map, logger log.Logger, debug bool) Container {
	c := &container{debug: debug, logger: logger, envs: envs}
	c.commonDeps = dependencies.NewCommonContainer(c, ctx)
	return c
}

type commonDeps = dependencies.Common

type container struct {
	commonDeps
	debug  bool
	logger log.Logger
	envs   *env.Map
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
