package persist

import (
	"context"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/persist"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
)

type Options struct {
	DryRun            bool
	LogUntrackedPaths bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApiClient() (client.Sender, error)
}

func Run(projectState *project.State, o Options, d dependencies) error {
	logger := d.Logger()

	// Get Storage API
	storageApiClient, err := d.StorageApiClient()
	if err != nil {
		return err
	}

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
		if err := plan.Invoke(d.Ctx(), logger, storageApiClient, projectState.State()); err != nil {
			return utils.PrefixError(`cannot persist objects`, err)
		}

		// Save manifest
		if _, err := saveManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
			return err
		}
	}

	// Print remaining untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(logger)
	}

	// Normalize paths
	if _, err := rename.Run(projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	logger.Info(`Persist done.`)
	return nil
}
