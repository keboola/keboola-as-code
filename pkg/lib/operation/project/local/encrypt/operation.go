package encrypt

import (
	"github.com/keboola/go-client/pkg/encryptionapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
)

type Options struct {
	DryRun   bool
	LogEmpty bool
}

type dependencies interface {
	Logger() log.Logger
	EncryptionApi() (*encryptionapi.Api, error)
}

func Run(projectState *project.State, o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Get Encryption API
	encryptionApi, err := d.EncryptionApi()
	if err != nil {
		return err
	}

	// Get plan
	plan := encrypt.NewPlan(projectState)

	// Log plan
	if !plan.Empty() || o.LogEmpty {
		plan.Log(logger)
	}

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, encryptionApi, projectState.State(), projectState.Ctx()); err != nil {
			return err
		}

		d.Logger().Info("Encrypt done.")
	}

	return nil
}
