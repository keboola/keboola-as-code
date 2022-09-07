package delete_template

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	deleteTemplate "github.com/keboola/keboola-as-code/internal/pkg/plan/delete-template"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	Branch   model.BranchKey
	DryRun   bool
	Instance string
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.template.delete")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get plan
	plan, err := deleteTemplate.NewPlan(projectState.State(), o.Branch, o.Instance)
	if err != nil {
		return err
	}

	// Log plan
	plan.Log(logger)

	// Dry run?
	if o.DryRun {
		logger.Info("Dry run, nothing changed.")
		return nil
	}

	// Invoke
	if err := plan.Invoke(ctx); err != nil {
		return utils.PrefixError(`cannot delete template configs`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(`Delete done.`)

	return nil
}
