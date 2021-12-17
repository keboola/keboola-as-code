package rename

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/rename"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/save"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	DryRun   bool
	LogEmpty bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() *zap.SugaredLogger
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
}

func Run(o Options, d dependencies) (changed bool, err error) {
	logger := d.Logger()

	// Load state
	loadOptions := loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
	projectState, err := d.LoadStateOnce(loadOptions)
	if err != nil {
		return false, err
	}

	// Get plan
	plan, err := rename.NewPlan(projectState)
	if err != nil {
		return false, err
	}

	// Log plan
	if o.LogEmpty || !plan.Empty() {
		plan.Log(log.ToInfoWriter(logger))
	}

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return false, nil
		}

		// Invoke
		if err := plan.Invoke(d.Ctx(), projectState.LocalManager()); err != nil {
			return false, utils.PrefixError(`cannot rename objects`, err)
		}

		// Save manifest
		if _, err := saveManifest.Run(d); err != nil {
			return false, err
		}

		logger.Info(`Rename done.`)
	}

	return !plan.Empty(), nil
}
