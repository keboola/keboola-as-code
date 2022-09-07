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

type Result struct {
	OldHash string
	NewHash string
	Changed bool
}

func Run(ctx context.Context, repo *git.Repository, d dependencies) (result *Result, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.repository.pull")
	defer telemetry.EndSpan(span, &err)

	// Context with timeout
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	// Get old hash
	oldHash, err := repo.CommitHash(ctx)
	if err != nil {
		return nil, err
	}

	// Pull
	if err := repo.Pull(ctx); err != nil {
		return nil, err
	}

	// Get new hash
	newHash, err := repo.CommitHash(ctx)
	if err != nil {
		return nil, err
	}

	// Done
	return &Result{OldHash: oldHash, NewHash: newHash, Changed: oldHash != newHash}, nil
}
