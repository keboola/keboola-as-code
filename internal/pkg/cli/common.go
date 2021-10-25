package cli

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/plan"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// diffProcessCmd run callback on diff results, common for pull, push ...
type diffProcessCmd struct {
	root                     *rootCommand
	cmd                      *cobra.Command
	action                   func(
		api *remote.StorageApi,
		schedulerApi *scheduler.Api,
		diffResults *diff.Results,
	) error
	onSuccess                func(api *remote.StorageApi)
	onError                  func(api *remote.StorageApi, err error)
	invalidStateCanBeIgnored bool
	ignoreInvalidState       bool
}

func (a *diffProcessCmd) run() error {
	successful := false
	logger := a.root.logger
	options := a.root.options

	// Validate project directory
	if err := ValidateMetadataFound(a.root.fs); err != nil {
		return err
	}

	// Validate token
	a.root.options.AskUser(a.root.prompt, "ApiToken")
	if err := a.root.ValidateOptions([]string{"ApiToken"}); err != nil {
		return err
	}

	// Load manifest
	projectManifest, err := manifest.LoadManifest(a.root.fs)
	if err != nil {
		return err
	}

	// Validate token and get API
	options.ApiHost = projectManifest.Project.ApiHost
	api, err := a.root.GetStorageApi()
	if err != nil {
		return err
	}

	// Send failed event on error
	defer func() {
		if err != nil && !successful && a.onError != nil {
			a.onError(api, err)
		}
	}()

	// Load project remote and local state
	stateOptions := state.NewOptions(projectManifest, api, a.root.ctx, logger)
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
				logger.Info(utils.PrefixError("Ignoring invalid local state", projectState.LocalErrors()))
			} else {
				errors := utils.PrefixError("project local state is invalid", projectState.LocalErrors())
				if a.invalidStateCanBeIgnored {
					errors.AppendRaw("\nUse --force to override the invalid local state.")
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

	token := api.Token().Token
	hostName, err := api.GetSchedulerApiUrl()
	if err != nil {
		return err
	}
	schedulerApi := scheduler.NewSchedulerApi(hostName, token, context.Background(), logger, true)

	// Run callback with diff results
	if err := a.action(api, schedulerApi, diffResults); err != nil {
		return err
	}

	// Success
	successful = true
	if a.onSuccess != nil {
		a.onSuccess(api)
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

func ValidateMetadataFound(fs filesystem.Fs) error {
	err := utils.NewMultiError()
	if !fs.IsDir(filesystem.MetadataDir) {
		err.Append(fmt.Errorf(`none of this and parent directories is project dir`))
		err.AppendRaw(`  Project directory must contain the ".keboola" metadata directory.`)
		err.AppendRaw(`  Please change working directory to a project directory or use the "init" command.`)
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
		encryptPlan := plan.Encrypt(projectState)
		if err := encryptPlan.ValidateAllEncrypted(); err != nil {
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

func Rename(projectState *state.State, logger *zap.SugaredLogger, logEmpty, dryRun bool) error {
	// Get plan
	rename, err := plan.Rename(projectState)
	if err != nil {
		return err
	}

	// Log plan
	if logEmpty || !rename.Empty() {
		rename.Log(log.ToInfoWriter(logger))
	}

	// Dry run?
	if dryRun {
		logger.Info("Dry run, nothing changed.")
		return nil
	}

	// Invoke
	if warn, err := rename.Invoke(logger, projectState.Manifest()); err != nil {
		return utils.PrefixError(`cannot rename objects`, err)
	} else if warn != nil {
		logger.Warn(`cannot finish objects renaming`, err)
	}

	if !rename.Empty() {
		logger.Info("Rename done.")
	}

	return nil
}
