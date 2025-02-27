package pull

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const Timeout = 30 * time.Second

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, repo *git.RemoteRepository, d dependencies) (result *git.PullResult, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.repository.pull")
	defer span.End(&err)

	// Context with timeout
	ctx, cancel := context.WithTimeoutCause(ctx, Timeout, errors.New("pull timeout"))
	defer cancel()

	// Pull
	result, err = repo.Pull(ctx)
	if result != nil {
		span.SetAttributes(
			attribute.String("templates.repository.id", repo.String()),
			attribute.String("templates.repository.url", repo.URL()),
			attribute.String("templates.repository.ref", repo.Ref()),
			attribute.String("templates.repository.oldHash", result.OldHash),
			attribute.String("templates.repository.newHash", result.NewHash),
			attribute.Bool("templates.repository.changed", result.Changed),
		)
	}

	return result, err
}
