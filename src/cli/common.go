package cli

import (
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"keboola-as-code/src/diff"
	"keboola-as-code/src/encryption"
	"keboola-as-code/src/log"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/plan"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/schema"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

// diffProcessCmd run callback on diff results, common for pull, push ...
type diffProcessCmd struct {
	root                     *rootCommand
	cmd                      *cobra.Command
	action                   func(api *remote.StorageApi, diffResults *diff.Results) error
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
	if err := a.root.ValidateOptions([]string{"projectDirectory"}); err != nil {
		return err
	}

	// Validate token
	a.root.options.AskUser(a.root.prompt, "ApiToken")
	if err := a.root.ValidateOptions([]string{"ApiToken"}); err != nil {
		return err
	}

	// Load manifest
	projectDir := options.ProjectDir()
	metadataDir := options.MetadataDir()
	projectManifest, err := manifest.LoadManifest(projectDir, metadataDir)
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

	// Run callback with diff results
	if err := a.action(api, diffResults); err != nil {
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
		logger.Debugf("Saved manifest file \"%s\".", utils.RelPath(projectManifest.ProjectDir, projectManifest.RelativePath()))
		return true, nil
	}
	return false, nil
}

func Validate(projectState *state.State, logger *zap.SugaredLogger) error {
	errors := utils.NewMultiError()
	if schemasErrors := schema.ValidateSchemas(projectState); schemasErrors != nil {
		errors.Append(schemasErrors)
	}
	if encryptionErrors := encryption.ValidateAllEncrypted(projectState); encryptionErrors != nil {
		errors.Append(encryptionErrors)
	}

	if err := errors.ErrorOrNil(); err != nil {
		return utils.PrefixError("validation failed", err)
	} else {
		logger.Debug("Validation done.")
	}
	return nil
}

func Rename(projectState *state.State, logger *zap.SugaredLogger, logEmpty, dryRun bool) error {
	// Get plan
	rename := plan.Rename(projectState)

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
	if warn, err := rename.Invoke(logger, projectState.ProjectDir(), projectState.Manifest()); err != nil {
		return utils.PrefixError(`cannot rename objects`, err)
	} else if warn != nil {
		logger.Warn(`cannot finish objects renaming`, err)
	}

	if !rename.Empty() {
		logger.Info("Rename done.")
	}

	return nil
}
