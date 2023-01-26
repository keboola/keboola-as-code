package check

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
)

type dependencies interface {
	Envs() env.Provider
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.version.check")
	defer telemetry.EndSpan(span, &err)

	return version.
		NewGitHubChecker(ctx, d.Logger(), d.Envs()).
		CheckIfLatest(build.BuildVersion)
}
