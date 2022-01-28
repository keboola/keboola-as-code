package pull

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/replacekeys"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/save"
)

type Options struct {
	RemoteFilter model.ObjectsFilter
	Replacements replacekeys.Keys
}

type dependencies interface {
	Logger() log.Logger
	TemplateDir() (filesystem.Fs, error)
	TemplateManifest() (*template.Manifest, error)
	TemplateState(loadOptions loadState.OptionsWithFilter, replacements replacekeys.Keys) (*template.State, error)
}

func LoadStateOptions(remoteFilter model.ObjectsFilter) loadState.OptionsWithFilter {
	return loadState.OptionsWithFilter{
		Options: loadState.Options{
			LoadLocalState:    true,
			LoadRemoteState:   true,
			IgnoreNotFoundErr: false,
		},
		RemoteFilter: &remoteFilter,
	}
}

func Run(o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Load state
	templateState, err := d.TemplateState(LoadStateOptions(o.RemoteFilter), o.Replacements)
	if err != nil {
		return err
	}

	// Diff
	results, err := createDiff.Run(createDiff.Options{Objects: templateState})
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
		// Invoke
		if err := plan.Invoke(logger, templateState.Ctx(), templateState.LocalManager(), templateState.RemoteManager(), ``); err != nil {
			return err
		}

		// Save manifest
		if _, err := saveManifest.Run(d); err != nil {
			return err
		}

		// Normalize paths

		// Validate schemas and encryption
	}

	if !plan.Empty() {
		logger.Info("Pull done.")
	}

	return nil
}
