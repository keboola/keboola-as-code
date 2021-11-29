package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/rename"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// diffProcessCmd run callback on diff results, common for pull, push ...
type diffProcessCmd struct {
	root   *rootCommand
	cmd    *cobra.Command
	action func(
		api *remote.StorageApi,
		diffResults *diff.Results,
	) error
	invalidStateCanBeIgnored bool
	ignoreInvalidState       bool
}

func (a *diffProcessCmd) run() error {
	logger := a.root.logger
	options := a.root.options

	// Validate project directory
	if err := ValidateMetadataFound(a.root.logger, a.root.fs); err != nil {
		return err
	}

	// Validate token
	a.root.options.AskUser(a.root.prompt, "ApiToken")
	if err := a.root.ValidateOptions([]string{"ApiToken"}); err != nil {
		return err
	}

	// Load manifest
	projectManifest, err := manifest.LoadManifest(a.root.fs, a.root.logger)
	if err != nil {
		return err
	}

	// Validate token and get API
	options.ApiHost = projectManifest.Project.ApiHost
	api, err := a.root.GetStorageApi()
	if err != nil {
		return err
	}

	// Get Scheduler API
	schedulerApi, err := a.root.GetSchedulerApi()
	if err != nil {
		return err
	}

	// Load project remote and local state
	stateOptions := state.NewOptions(projectManifest, api, schedulerApi, a.root.ctx, logger)
	stateOptions.LoadLocalState = true
	stateOptions.LoadRemoteState = true
	projectState, ok := state.LoadState(stateOptions)
	if ok {
		logger.Debugf("Project local and remote states have been successfully loaded.")
	} else {
		if projectState.RemoteErrors().Len() > 0 {
			return utils.PrefixError("cannot load project remote state", projectState.RemoteErrors())
		}
		if projectState.LocalErrors().Len() > 0 {
			if a.ignoreInvalidState {
				logger.Info(`Ignoring invalid local state.`)
			} else {
				errors := utils.PrefixError("project local state is invalid", projectState.LocalErrors())
				if a.invalidStateCanBeIgnored {
					logger.Info()
					logger.Info("Use --force to override the invalid local state.")
				}
				return errors
			}
		}
	}

	// Diff
	differ := diff.NewDiffer(projectState)
	diffResults, err := differ.Diff()
	if err != nil {
		return err
	}

	// Run callback with diff results
	if err := a.action(api, diffResults); err != nil {
		return err
	}

	return nil
}

func SaveManifest(projectManifest *manifest.Manifest, logger *zap.SugaredLogger) (bool, error) {
	if projectManifest.IsChanged() {
		if err := projectManifest.Save(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func ValidateMetadataFound(logger *zap.SugaredLogger, fs filesystem.Fs) error {
	err := utils.NewMultiError()
	if !fs.IsDir(filesystem.MetadataDir) {
		err.Append(fmt.Errorf(`none of this and parent directories is project dir`))
		logger.Info(`Project directory must contain the ".keboola" metadata directory.`)
		logger.Info(`Please change working directory to a project directory or use the "init" command.`)
	}

	return err.ErrorOrNil()
}

func Validate(projectState *state.State, logger *zap.SugaredLogger, skipEncryptValidation bool) error {
	errors := utils.NewMultiError()

	// Validate schemas
	if err := schema.ValidateSchemas(projectState); err != nil {
		errors.Append(err)
	}

	if !skipEncryptValidation {
		// Validate all values encrypted
		plan := encrypt.NewPlan(projectState)
		if err := plan.ValidateAllEncrypted(); err != nil {
			errors.Append(err)
		}
	}

	// Process errors
	if err := errors.ErrorOrNil(); err != nil {
		return utils.PrefixError("validation failed", err)
	} else {
		logger.Debug("Validation done.")
	}
	return nil
}

func Rename(ctx context.Context, projectState *state.State, logger *zap.SugaredLogger, logEmpty, dryRun bool) error {
	// Get plan
	plan, err := rename.NewPlan(projectState)
	if err != nil {
		return err
	}

	// Log plan
	if logEmpty || !plan.Empty() {
		plan.Log(log.ToInfoWriter(logger))
	}

	// Dry run?
	if dryRun {
		logger.Info("Dry run, nothing changed.")
		return nil
	}

	// Invoke
	if err := plan.Invoke(ctx, projectState.LocalManager()); err != nil {
		return utils.PrefixError(`cannot rename objects`, err)
	}

	if !plan.Empty() {
		logger.Info("Rename done.")
	}

	return nil
}
