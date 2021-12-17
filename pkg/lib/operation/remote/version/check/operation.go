package check

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
)

type dependencies interface {
	Ctx() context.Context
	Logger() *zap.SugaredLogger
	Envs() *env.Map
}

func Run(d dependencies) (err error) {
	return version.
		NewGitHubChecker(d.Ctx(), d.Logger(), d.Envs()).
		CheckIfLatest(build.BuildVersion)
}
