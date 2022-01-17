package encrypt

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/project/state/load"
)

type Options struct {
	DryRun bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	EncryptionApi() (*encryption.Api, error)
	ProjectManifest() (*manifest.Manifest, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
}

func Run(o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Get Encryption API
	encryptionApi, err := d.EncryptionApi()
	if err != nil {
		return err
	}

	// Load state
	loadOptions := loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
	// Load state
	projectState, err := d.ProjectState(loadOptions)
	if err != nil {
		return err
	}

	// Get plan
	plan := encrypt.NewPlan(projectState)

	// Log plan
	plan.Log(logger)

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, encryptionApi, projectState.State(), d.Ctx()); err != nil {
			return err
		}

		d.Logger().Info("Encrypt done.")
	}

	return nil
}
