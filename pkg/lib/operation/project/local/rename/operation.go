package rename

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/rename"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	DryRun   bool
	LogEmpty bool
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (changed bool, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.rename")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get plan
	plan, err := rename.NewPlan(projectState.State())
	if err != nil {
		return false, err
	}

	// Log plan
	if o.LogEmpty || !plan.Empty() {
		plan.Log(logger)
	}

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return false, nil
		}

		// Invoke
		if err := plan.Invoke(projectState.Ctx(), projectState.LocalManager()); err != nil {
			return false, errors.PrefixError(err, "cannot rename objects")
		}

		// Save manifest
		if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
			return false, err
		}

		logger.Info(`Rename done.`)
	}

	return !plan.Empty(), nil
}
