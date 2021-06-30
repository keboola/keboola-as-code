package cli

import (
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
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
		if err != nil && !successful {
			a.onError(api, err)
		}
	}()

	// Load project remote and local state
	projectState, ok := state.LoadState(projectManifest, logger, a.root.ctx, api, true)
	if ok {
		logger.Debugf("Project local and remote states have been successfully loaded.")
	} else {
		if projectState.RemoteErrors().Len() > 0 {
			return utils.WrapError("cannot load project remote state", projectState.RemoteErrors())
		}
		if projectState.LocalErrors().Len() > 0 {
			if a.ignoreInvalidState {
				logger.Info(utils.WrapError("Ignoring invalid local state", projectState.LocalErrors()))
			} else {
				errors := utils.WrapError("project local state is invalid", projectState.LocalErrors())
				if a.invalidStateCanBeIgnored {
					errors.AddRaw("\nUse --force to override the invalid local state.")
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
	a.onSuccess(api)
	return nil
}
