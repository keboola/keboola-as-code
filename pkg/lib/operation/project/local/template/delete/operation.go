package delete_template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	deleteTemplate "github.com/keboola/keboola-as-code/internal/pkg/plan/delete-template"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	Branch   model.BranchKey
	DryRun   bool
	Instance string
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
}

func Run(projectState *project.State, o Options, d dependencies) error {
	logger := d.Logger()

	// Get plan
	plan, err := deleteTemplate.NewPlan(projectState.State(), o.Branch, o.Instance)
	if err != nil {
		return err
	}

	// Log plan
	plan.Log(logger)

	// Dry run?
	if o.DryRun {
		logger.Info("Dry run, nothing changed.")
		return nil
	}

	// Invoke
	if err := plan.Invoke(projectState.Ctx()); err != nil {
		return utils.PrefixError(`cannot delete template configs`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(`Delete done.`)

	return nil
}
