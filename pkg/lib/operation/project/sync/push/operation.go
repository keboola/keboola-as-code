package push

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/push"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/ignore"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	pullop "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
)

type Options struct {
	Encrypt           bool
	DryRun            bool
	SkipValidation    bool
	AllowRemoteDelete bool
	LogUntrackedPaths bool
	ChangeDescription string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.push")
	defer span.End(&err)

	logger := d.Logger()

	// Encrypt before push?
	if o.Encrypt {
		if err := encrypt.Run(ctx, projectState, encrypt.Options{DryRun: o.DryRun, LogEmpty: true}, d); err != nil {
			return err
		}
	}

	// Change description - optional arg
	logger.Debugf(ctx, `Change description: "%s"`, o.ChangeDescription)

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(ctx, logger)
	}

	// Validate
	if !o.SkipValidation {
		validateOptions := validate.Options{
			ValidateSecrets:    !o.Encrypt || !o.DryRun,
			ValidateJSONSchema: true,
		}
		if err := validate.Run(ctx, projectState, validateOptions, d); err != nil {
			return err
		}
	}

	if projectState.Fs().Exists(ctx, ignore.KBCIgnoreFilePath) {
		// Load ignore file
		file, err := ignore.LoadFile(ctx, projectState.Fs(), projectState.Registry, ignore.KBCIgnoreFilePath)
		if err != nil {
			return err
		}

		if err = file.IgnoreConfigsOrRows(); err != nil {
			return err
		}

		// Also propagate ignore to orchestrators referencing ignored configs
		pullop.IgnoreConfigsAndRows(projectState)
	}

	// Diff
	results, err := createDiff.Run(ctx, createDiff.Options{Objects: projectState}, d, diff.WithIgnoreBranchName(projectState.ProjectManifest().AllowTargetENV()))
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
	plan.Log(d.Stdout())

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info(ctx, "Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, ctx, projectState.LocalManager(), projectState.RemoteManager(), o.ChangeDescription); err != nil {
			return err
		}

		logger.Info(ctx, "Push done.")
	}
	return nil
}
