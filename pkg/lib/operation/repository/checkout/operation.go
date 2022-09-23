package checkout

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const Timeout = 30 * time.Second

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, def model.TemplateRepository, d dependencies) (repo *git.RemoteRepository, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.repository.checkout")
	span.SetAttributes(telemetry.KeepSpan())
	defer telemetry.EndSpan(span, &err)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	// Checkout
	repo, err = git.Checkout(ctx, def, false, d.Logger())
	if err != nil {
		return nil, fmt.Errorf(`cannot checkout out repository "%s": %w`, def, err)
	} else {
		span.SetAttributes(attribute.String("kac.repository.id", repo.String()))
		span.SetAttributes(attribute.String("kac.repository.url", repo.Url()))
		span.SetAttributes(attribute.String("kac.repository.ref", repo.Ref()))
		span.SetAttributes(attribute.String("kac.repository.oldHash", ""))
		span.SetAttributes(attribute.String("kac.repository.newHash", repo.CommitHash()))
		span.SetAttributes(attribute.Bool("kac.repository.changed", true))
	}

	// Done
	return repo, nil
}
