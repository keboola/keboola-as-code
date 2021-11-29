package persist

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/persist"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
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
	Logger() *zap.SugaredLogger
	Fs() filesystem.Fs
	StorageApi() (*remote.StorageApi, error)
	Manifest() (*manifest.Manifest, error)
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
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
	projectState, err := d.LoadStateOnce(LoadStateOptions())
	if err != nil {
		return err
	}

	// Get plan
	plan, err := persist.NewPlan(projectState)
	if err != nil {
		return err
	}

	// Log plan
	plan.Log(log.ToInfoWriter(logger))

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, storageApi, projectState); err != nil {
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
