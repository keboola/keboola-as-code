package push

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/push"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/encrypt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/validate"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/sync/diff/create"
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
	Logger() *zap.SugaredLogger
	EncryptionApi() (*encryption.Api, error)
	Manifest() (*manifest.Manifest, error)
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
}

func LoadStateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(o Options, d dependencies) error {
	ctx := d.Ctx()
	logger := d.Logger()

	// Load state
	projectState, err := d.LoadStateOnce(LoadStateOptions())
	if err != nil {
		return err
	}

	// Encrypt before push?
	if o.Encrypt {
		if err := encrypt.Run(encrypt.Options{DryRun: o.DryRun}, d); err != nil {
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
	if err := validate.Run(validateOptions, d); err != nil {
		return err
	}

	// Diff
	results, err := createDiff.Run(createDiff.Options{State: projectState})
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
	plan.Log(log.ToInfoWriter(logger))

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, ctx, o.ChangeDescription); err != nil {
			return err
		}

		logger.Info("Push done.")
	}
	return nil
}
