package delete_template

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	deleteTemplate "github.com/keboola/keboola-as-code/internal/pkg/plan/delete-template"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	Branch   model.BranchKey
	DryRun   bool
	Instance string
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.delete")
	defer span.End(&err)

	logger := d.Logger()

	// Get plan
	plan, err := deleteTemplate.NewPlan(projectState.State(), o.Branch, o.Instance)
	if err != nil {
		return err
	}

	// Log plan
	plan.Log(d.Stdout())

	// Dry run?
	if o.DryRun {
		logger.Info(ctx, "Dry run, nothing changed.")
		return nil
	}

	// Invoke
	if err := plan.Invoke(ctx); err != nil {
		return errors.PrefixError(err, "cannot delete template configs")
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(ctx, `Delete done.`)

	return nil
}
