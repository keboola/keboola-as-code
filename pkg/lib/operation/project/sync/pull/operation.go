package pull

import (
	"context"
	"io"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/cachefile"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	DryRun            bool
	LogUntrackedPaths bool
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
	ProjectBackends() []string
	ProjectFeatures() keboola.FeaturesMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func LoadStateOptions(force bool) loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: force,
	}
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.pull")
	defer span.End(&err)

	logger := d.Logger()

	// Diff
	results, err := createDiff.Run(ctx, createDiff.Options{Objects: projectState}, d, diff.WithAllowTargetEnv(projectState.ProjectManifest().AllowTargetENV()))
	if err != nil {
		return err
	}

	// Get plan
	plan, err := pull.NewPlan(results)
	if err != nil {
		return err
	}

	// Get default branch
	defaultBranch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
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
		if err := plan.Invoke(logger, projectState.Ctx(), projectState.LocalManager(), projectState.RemoteManager(), ``); err != nil { // nolint: contextcheck
			return err
		}

		// Save manifest
		if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
			return err
		}

		// Save project.json
		if err := cachefile.New().Save(ctx, projectState.Fs(), d.ProjectBackends(), d.ProjectFeatures(), defaultBranch.ID); err != nil {
			return err
		}

		// Normalize paths
		if _, err := rename.Run(ctx, projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
			return err
		}

		// Validate schemas and encryption
		if err := validate.Run(ctx, projectState, validate.Options{ValidateSecrets: true, ValidateJSONSchema: true}, d); err != nil {
			logger.Warn(ctx, errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
			logger.Warn(ctx, "")
			logger.Warn(ctx, `The project has been pulled, but it is not in a valid state.`)
			logger.Warn(ctx, `Please correct the problems listed above.`)
			logger.Warn(ctx, `Push operation is only possible when project is valid.`)
		}
	}

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(ctx, logger)
	}

	if !plan.Empty() {
		logger.Info(ctx, "Pull done.")
	}

	return nil
}
