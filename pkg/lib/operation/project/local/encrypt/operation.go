package encrypt

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	DryRun   bool
	LogEmpty bool
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.encrypt")
	defer span.End(&err)

	logger := d.Logger()

	// Get API
	api := d.KeboolaProjectAPI()

	// Get plan
	plan := encrypt.NewPlan(projectState)

	// Log plan
	if !plan.Empty() || o.LogEmpty {
		plan.Log(d.Stdout())
	}

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info(ctx, "Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(ctx, d.ProjectID(), logger, api, projectState.State()); err != nil {
			return err
		}

		d.Logger().Info(ctx, "Encrypt done.")
	}

	return nil
}
