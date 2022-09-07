package checkout

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const Timeout = 30 * time.Second

type dependencies interface {
	Logger() log.Logger
}

func Run(ctx context.Context, repoDef model.TemplateRepository, d dependencies) (*git.Repository, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	// Checkout
	repo, err := git.Checkout(ctx, repoDef.Url, repoDef.Ref, false, d.Logger())
	if err != nil {
		return nil, fmt.Errorf(`cannot checkout out repository "%s": %w`, repoDef, err)
	}

	// Done
	return repo, nil
}
