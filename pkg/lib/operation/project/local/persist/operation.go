package persist

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/persist"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
)

type Options struct {
	DryRun            bool
	LogUntrackedPaths bool
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.persist")
	defer span.End(&err)

	logger := d.Logger()

	// Get Storage API
	api := d.KeboolaProjectAPI()

	// Get plan
	plan, err := persist.NewPlan(ctx, projectState.State())
	if err != nil {
		return err
	}

	// Log plan
	plan.Log(d.Stdout())

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info(ctx, "Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(ctx, logger, api, projectState.State()); err != nil {
			return errors.PrefixError(err, "cannot persist objects")
		}

		// Save manifest
		if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
			return err
		}
	}

	// Print remaining untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(ctx, logger)
	}

	// Normalize paths
	if _, err := rename.Run(ctx, projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	logger.Info(ctx, `Persist done.`)
	return nil
}
