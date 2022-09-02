package check

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
)

type dependencies interface {
	Logger() log.Logger
	Envs() env.Provider
}

func Run(ctx context.Context, d dependencies) (err error) {
	return version.
		NewGitHubChecker(ctx, d.Logger(), d.Envs()).
		CheckIfLatest(build.BuildVersion)
}
