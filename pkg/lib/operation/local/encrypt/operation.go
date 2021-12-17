package encrypt

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	DryRun bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() *zap.SugaredLogger
	EncryptionApi() (*encryption.Api, error)
	ProjectManifest() (*manifest.Manifest, error)
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
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
	projectState, err := d.LoadStateOnce(loadOptions)
	if err != nil {
		return err
	}

	// Get plan
	plan := encrypt.NewPlan(projectState)

	// Log plan
	plan.Log(log.ToInfoWriter(logger))

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, encryptionApi, d.Ctx()); err != nil {
			return err
		}

		d.Logger().Info("Encrypt done.")
	}

	return nil
}
