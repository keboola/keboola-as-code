package checkout

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const Timeout = 30 * time.Second

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, def model.TemplateRepository, d dependencies) (repo *git.RemoteRepository, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.repository.checkout")
	defer span.End(&err)

	// Create context with timeout
	ctx, cancel := context.WithTimeoutCause(ctx, Timeout, errors.New("checkout timeout"))
	defer cancel()

	// Checkout
	repo, err = git.Checkout(ctx, def, false, d.Logger())
	if err != nil {
		return nil, errors.Errorf(`cannot checkout out repository "%s": %w`, def, err)
	} else {
		span.SetAttributes(
			attribute.String("templates.repository.id", repo.String()),
			attribute.String("templates.repository.url", repo.URL()),
			attribute.String("templates.repository.ref", repo.Ref()),
			attribute.String("templates.repository.oldHash", ""),
			attribute.String("templates.repository.newHash", repo.CommitHash()),
			attribute.Bool("templates.repository.changed", true),
		)
	}

	// Done
	return repo, nil
}
