package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"time"
)

// diffResultsAction run callback on diff results, common for pull and push command
type diffResultsAction struct {
	root      *rootCommand
	cmd       *cobra.Command
	action    func(api *remote.StorageApi, projectManifest *manifest.Manifest, projectState *state.State, diffResults *diff.Results) error
	onSuccess func(api *remote.StorageApi)
	onError   func(api *remote.StorageApi, err error)
}

func (a *diffResultsAction) run() error {
	successful := false
	logger := a.root.logger

	// Validate token and get API
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

	// Load manifest
	projectDir := a.root.options.ProjectDir()
	metadataDir := a.root.options.MetadataDir()
	projectManifest, err := manifest.LoadManifest(projectDir, metadataDir)
	if err != nil {
		return err
	}

	// Load project remote and local state
	projectState, ok := state.LoadState(projectManifest, logger, a.root.ctx, api)
	if ok {
		logger.Debugf("Project local and remote states have been successfully loaded.")
	} else {
		if projectState.RemoteErrors().Len() > 0 {
			return fmt.Errorf("cannot load project remote state: %s", projectState.RemoteErrors())
		}
		if projectState.LocalErrors().Len() > 0 {
			force, _ := a.cmd.Flags().GetBool("force")
			if force {
				logger.Infof("Ignoring invalid local state:%s", projectState.LocalErrors())
			} else {
				msg := "project local state is invalid"
				if a.cmd.Flags().Lookup("force") != nil {
					return fmt.Errorf(
						"%s:%s\n\n%s",
						msg,
						projectState.LocalErrors(),
						"Use --force to override the invalid local state.",
					)
				} else {
					return fmt.Errorf(
						"%s:%s",
						msg,
						projectState.LocalErrors(),
					)
				}
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
	if err := a.action(api, projectManifest, projectState, diffResults); err != nil {
		return err
	}

	// Success
	successful = true
	a.onSuccess(api)
	return nil
}

func sendCmdSuccessfulEvent(root *rootCommand, api *remote.StorageApi, cmd, msg string) {
	duration := time.Since(root.start)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
	}
	event, err := api.CreateEvent("info", msg, duration, params, results)
	if err == nil {
		root.logger.Debugf("Sent \"%s\" successful event id: \"%s\"", cmd, event.Id)
	} else {
		root.logger.Warnf("Cannot send \"%s\" successful event: %s", cmd, err)
	}
}

func sendCmdFailedEvent(root *rootCommand, api *remote.StorageApi, err error, cmd, msg string) {
	duration := time.Since(root.start)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := api.CreateEvent("error", msg, duration, params, results)
	if err == nil {
		root.logger.Debugf("Sent \"%s\" failed event id: \"%s\"", cmd, event.Id)
	} else {
		root.logger.Warnf("Cannot send \"%s\" failed event: %s", cmd, err)
	}
}
