package pull

import (
	"context"
	"time"

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
	defer telemetry.EndSpan(span, &err)

	// Context with timeout
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	// Pull
	return repo.Pull(ctx)
}
