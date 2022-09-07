package pull

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const PullTimeout = 30 * time.Second

type dependencies interface {
	Logger() log.Logger
}

type Result struct {
	OldHash string
	NewHash string
	Changed bool
}

func Run(ctx context.Context, repo *git.Repository, d dependencies) (*Result, error) {
	// Context with timeout
	ctx, cancel := context.WithTimeout(ctx, PullTimeout)
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
