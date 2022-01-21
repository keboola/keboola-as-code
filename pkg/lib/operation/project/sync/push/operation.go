package push

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/push"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
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
	EncryptionApi() (*encryption.Api, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
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
	projectState, err := d.ProjectState(LoadStateOptions())
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
