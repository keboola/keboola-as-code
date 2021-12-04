package pull

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/validate"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/sync/diff/create"
)

type Options struct {
	DryRun            bool
	Force             bool // ignore invalid local state
	LogUntrackedPaths bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() *zap.SugaredLogger
	ProjectManifest() (*manifest.Manifest, error)
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
}

func LoadStateOptions(force bool) loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: force,
	}
}

func Run(o Options, d dependencies) (err error) {
	ctx := d.Ctx()
	logger := d.Logger()

	// Load state
	projectState, err := d.LoadStateOnce(LoadStateOptions(o.Force))
	if err != nil {
		return err
	}

	// Diff
	results, err := createDiff.Run(createDiff.Options{State: projectState})
	if err != nil {
		return err
	}

	// Get plan
	plan, err := pull.NewPlan(results)
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
		if err := plan.Invoke(logger, ctx, ``); err != nil {
			return err
		}

		// Save manifest
		if _, err := saveManifest.Run(d); err != nil {
			return err
		}

		// Normalize paths
		if _, err := rename.Run(rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
			return err
		}

		// Validate schemas and encryption
		if err := validate.Run(validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
			logger.Warn(`Warning, ` + err.Error())
			logger.Warn()
			logger.Warnf(`The project has been pulled, but it is not in a valid state.`)
			logger.Warnf(`Please correct the problems listed above.`)
			logger.Warnf(`Push operation is only possible when project is valid.`)
		}
	}

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(logger)
	}

	if !plan.Empty() {
		logger.Info("Pull done.")
	}

	return nil
}
