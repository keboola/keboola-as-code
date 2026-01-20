package pull

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/cachefile"
	"github.com/keboola/keboola-as-code/internal/pkg/project/ignore"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	DryRun                 bool
	LogUntrackedPaths      bool
	CleanupRenameConflicts bool
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

	if projectState.Fs().Exists(ctx, ignore.KBCIgnoreFilePath) {
		// Load ignore file
		file, err := ignore.LoadFile(ctx, projectState.Fs(), projectState.Registry, ignore.KBCIgnoreFilePath)
		if err != nil {
			return err
		}

		if err = file.IgnoreConfigsOrRows(); err != nil {
			return err
		}

		ignoreConfigsAndRows(projectState)
	}

	// Diff
	results, err := createDiff.Run(ctx, createDiff.Options{Objects: projectState}, d, diff.WithIgnoreBranchName(projectState.ProjectManifest().AllowTargetENV()))
	if err != nil {
		return err
	}

	var renameOptions rename.Options
	if o.CleanupRenameConflicts {
		renameOptions.Cleanup = true
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

	if plan.Empty() {
		// Log untracked paths
		if o.LogUntrackedPaths {
			projectState.LogUntrackedPaths(ctx, logger)
		}
		return nil
	}

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

	// Reload local state so that the naming registry reflects deletions
	// and freed paths before running path normalization. Without this the
	// rename plan may see old paths as still occupied and append suffixes.
	if ok, localErr, _ := projectState.Load(ctx, state.LoadOptions{LoadLocalState: true}); !ok {
		if localErr != nil {
			return localErr
		}
		return errors.New("failed to reload local state")
	}

	// Normalize paths
	// Enable Cleanup to handle chained renames where destination temporarily exists
	if _, err := rename.Run(ctx, projectState, renameOptions, d); err != nil {
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

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(ctx, logger)
	}

	logger.Info(ctx, "Pull done.")

	return nil
}

func ignoreConfigsAndRows(projectState *project.State) {
	for _, v := range projectState.IgnoredConfigRows() {
		v.SetRemoteState(nil)
	}

	for _, v := range projectState.IgnoredConfigs() {
		v.SetRemoteState(nil)
	}
}
