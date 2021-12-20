package persist

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/persist"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	DryRun            bool
	LogUntrackedPaths bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
}

func LoadStateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       true,
		IgnoreInvalidLocalState: false,
	}
}

func Run(o Options, d dependencies) error {
	logger := d.Logger()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Load state
	projectState, err := d.ProjectState(LoadStateOptions())
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
		if err := plan.Invoke(logger, storageApi, projectState.State()); err != nil {
			return utils.PrefixError(`cannot persist objects`, err)
		}

		// Save manifest
		if _, err := saveManifest.Run(d); err != nil {
			return err
		}
	}

	// Print remaining untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(logger)
	}

	// Normalize paths
	if _, err := rename.Run(rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	logger.Info(`Persist done.`)
	return nil
}
