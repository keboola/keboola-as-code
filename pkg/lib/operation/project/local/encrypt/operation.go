package encrypt

import (
	"github.com/keboola/go-client/pkg/client"

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
	ProjectID() (int, error)
	EncryptionApiClient() (client.Sender, error)
}

func Run(projectState *project.State, o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Get project ID
	projectID, err := d.ProjectID()
	if err != nil {
		return err
	}

	// Get Encryption API
	encryptionApiClient, err := d.EncryptionApiClient()
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
		if err := plan.Invoke(projectState.Ctx(), projectID, logger, encryptionApiClient, projectState.State()); err != nil {
			return err
		}

		d.Logger().Info("Encrypt done.")
	}

	return nil
}
