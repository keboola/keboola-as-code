package push

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/push"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
)

type Options struct {
	Encrypt           bool
	DryRun            bool
	AllowRemoteDelete bool
	LogUntrackedPaths bool
	ChangeDescription string
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	EncryptionApi() (*encryptionapi.Api, error)
}

func Run(projectState *project.State, o Options, d dependencies) error {
	ctx := d.Ctx()
	logger := d.Logger()

	// Encrypt before push?
	if o.Encrypt {
		if err := encrypt.Run(projectState, encrypt.Options{DryRun: o.DryRun, LogEmpty: true}, d); err != nil {
			return err
		}
	}

	// Change description - optional arg
	logger.Debugf(`Change description: "%s"`, o.ChangeDescription)

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(logger)
	}

	// Validate
	validateOptions := validate.Options{
		ValidateSecrets:    !o.Encrypt || !o.DryRun,
		ValidateJsonSchema: true,
	}
	if err := validate.Run(projectState, validateOptions, d); err != nil {
		return err
	}

	// Diff
	results, err := createDiff.Run(createDiff.Options{Objects: projectState})
	if err != nil {
		return err
	}

	// Get plan
	plan, err := push.NewPlan(results)
	if err != nil {
		return err
	}

	// Allow remote deletion, if --force
	if o.AllowRemoteDelete {
		plan.AllowRemoteDelete()
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
		if err := plan.Invoke(logger, ctx, projectState.LocalManager(), projectState.RemoteManager(), o.ChangeDescription); err != nil {
			return err
		}

		logger.Info("Push done.")
	}
	return nil
}
