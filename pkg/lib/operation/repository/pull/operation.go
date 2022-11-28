package pull

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const Timeout = 30 * time.Second

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, repo *git.RemoteRepository, d dependencies) (result *git.PullResult, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.repository.pull")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	// Context with timeout
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	// Pull
	result, err = repo.Pull(ctx)
	if result != nil {
		span.SetAttributes(attribute.String("kac.repository.id", repo.String()))
		span.SetAttributes(attribute.String("kac.repository.url", repo.URL()))
		span.SetAttributes(attribute.String("kac.repository.ref", repo.Ref()))
		span.SetAttributes(attribute.String("kac.repository.oldHash", result.OldHash))
		span.SetAttributes(attribute.String("kac.repository.newHash", result.NewHash))
		span.SetAttributes(attribute.Bool("kac.repository.changed", result.Changed))
	}

	return result, err
}
