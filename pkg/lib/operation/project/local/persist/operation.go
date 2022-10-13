package persist

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"go.opentelemetry.io/otel/trace"

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
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.persist")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get Storage API
	storageApiClient := d.StorageApiClient()

	// Get plan
	plan, err := persist.NewPlan(projectState.State())
	if err != nil {
		return err
	}

	// Log plan
	plan.Log(logger)

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(ctx, logger, storageApiClient, projectState.State()); err != nil {
			return errors.PrefixError(err, "cannot persist objects")
		}

		// Save manifest
		if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
			return err
		}
	}

	// Print remaining untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(logger)
	}

	// Normalize paths
	if _, err := rename.Run(ctx, projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	logger.Info(`Persist done.`)
	return nil
}
