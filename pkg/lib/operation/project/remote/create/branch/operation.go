package branch

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Options struct {
	Name string
	Pull bool
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (branch *keboola.Branch, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.create.branch")
	defer span.End(&err)

	logger := d.Logger()

	// Create branch by API
	branch = &keboola.Branch{Name: o.Name}
	if _, err := d.KeboolaProjectAPI().CreateBranchRequest(branch).Send(ctx); err != nil {
		return nil, errors.Errorf(`cannot create branch: %w`, err)
	}

	logger.Infof(ctx, `Created new branch "%s".`, branch.Name)
	return branch, nil
}
