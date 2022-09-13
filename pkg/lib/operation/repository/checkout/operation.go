package checkout

import (
	"context"
	"fmt"
	"time"

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
	defer telemetry.EndSpan(span, &err)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	// Checkout
	repo, err = git.Checkout(ctx, def, false, d.Logger())
	if err != nil {
		return nil, fmt.Errorf(`cannot checkout out repository "%s": %w`, def, err)
	}

	// Done
	return repo, nil
}
