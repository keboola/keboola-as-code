package pull

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	DryRun            bool
	LogUntrackedPaths bool
}

type dependencies interface {
	Logger() log.Logger
}

func LoadStateOptions(force bool) loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: force,
	}
}

func Run(projectState *project.State, o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Diff
	results, err := createDiff.Run(createDiff.Options{Objects: projectState})
	if err != nil {
		return err
	}

	// Get plan
	plan, err := pull.NewPlan(results)
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
		if err := plan.Invoke(logger, projectState.Ctx(), projectState.LocalManager(), projectState.RemoteManager(), ``); err != nil {
			return err
		}

		// Save manifest
		if _, err := saveManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
			return err
		}

		// Normalize paths
		if _, err := rename.Run(projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
			return err
		}

		// Validate schemas and encryption
		if err := validate.Run(projectState, validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
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
