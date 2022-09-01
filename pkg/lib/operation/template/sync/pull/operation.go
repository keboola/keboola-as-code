package pull

import (
	"context"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/save"
)

type Options struct {
	Context template.Context
}

type dependencies interface {
	Logger() log.Logger
	Components() *model.ComponentsMap
	StorageApiClient() client.Sender
	SchedulerApiClient() client.Sender
}

func LoadStateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:    true,
		LoadRemoteState:   true,
		IgnoreNotFoundErr: false,
	}
}

func Run(ctx context.Context, tmpl *template.Template, o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Load state
	templateState, err := tmpl.LoadState(o.Context, LoadStateOptions(), d)
	if err != nil {
		return err
	}

	// Diff
	results, err := createDiff.Run(ctx, createDiff.Options{Objects: templateState})
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
		if _, err := saveManifest.Run(ctx, templateState.TemplateManifest(), templateState.Fs(), d); err != nil {
			return err
		}
	}

	if !plan.Empty() {
		logger.Info("Pull done.")
	}

	return nil
}
