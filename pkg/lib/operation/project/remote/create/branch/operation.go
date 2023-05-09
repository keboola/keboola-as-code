package branch

import (
	"context"
	"fmt"

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
	ctx, span := d.Telemetry().Tracer().Start(ctx, "kac.lib.operation.project.remote.create.branch")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Create branch by API
	branch = &keboola.Branch{Name: o.Name}
	if _, err := d.KeboolaProjectAPI().CreateBranchRequest(branch).Send(ctx); err != nil {
		return nil, errors.Errorf(`cannot create branch: %w`, err)
	}

	logger.Info(fmt.Sprintf(`Created new branch "%s".`, branch.Name))
	return branch, nil
}
