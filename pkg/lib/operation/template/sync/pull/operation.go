package pull

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/save"
	loadStateOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/state/load"
)

type Options struct {
	Template *template.Template
	Context  template.Context
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	TemplateState(options loadStateOp.Options) (*template.State, error)
}

func LoadStateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:    true,
		LoadRemoteState:   true,
		IgnoreNotFoundErr: false,
	}
}

func Run(o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Load state
	templateState, err := d.TemplateState(loadStateOp.Options{
		Template:    o.Template,
		Context:     o.Context,
		LoadOptions: LoadStateOptions(),
	})
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
		if _, err := saveManifest.Run(templateState.TemplateManifest(), templateState.Fs(), d); err != nil {
			return err
		}
	}

	if !plan.Empty() {
		logger.Info("Pull done.")
	}

	return nil
}
