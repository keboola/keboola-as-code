package check

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
)

type dependencies interface {
	Envs() env.Provider
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.version.check")
	defer telemetry.EndSpan(span, &err)

	return version.
		NewGitHubChecker(ctx, d.Logger(), d.Envs()).
		CheckIfLatest(build.BuildVersion)
}
