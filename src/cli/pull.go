package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/model"
	"keboola-as-code/src/plan"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
	"time"
)

const pullShortDescription = `Pull configurations to the local project dir`
const pullLongDescription = `Command "pull"

Pull configurations from the Keboola Connection project.
Local files will be overwritten to match the project's state.

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`

func pullCommand(root *rootCommand) *cobra.Command {
	force := false
	dryRun := false

	cmd := &cobra.Command{
		Use:   "pull",
		Short: pullShortDescription,
		Long:  pullLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Ask for the host/token, if not specified -> to make the first step easier
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")

			// Validate options
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiHost", "ApiToken"}); err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			successful := false
			logger := root.logger

			// Validate token and get API
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Send failed event on error
			defer func() {
				if err != nil && !successful {
					sendPullFailedEvent(root, api, err)
				}
			}()

			// Load manifest
			projectDir := root.options.ProjectDir()
			metadataDir := root.options.MetadataDir()
			manifest, err := model.LoadManifest(projectDir, metadataDir)
			if err != nil {
				return err
			}

			// Load project remote and local state
			projectState, ok := state.LoadState(manifest, logger, root.ctx, api)
			if ok {
				logger.Debugf("Project local and remote states successfully loaded.")
			} else {
				if projectState.RemoteErrors().Len() > 0 {
					logger.Debugf("Project remote state load failed: %s", projectState.RemoteErrors())
					return fmt.Errorf("cannot load project remote state: %s", projectState.RemoteErrors())
				}
				if projectState.LocalErrors().Len() > 0 {
					if force {
						logger.Infof("Ignoring invalid local state:%s", projectState.LocalErrors())
					} else {
						return fmt.Errorf(
							"project local state is invalid:%s\n\n%s",
							projectState.LocalErrors(),
							"Use --force to override the invalid local state.",
						)
					}
				}
			}

			// Log untracked paths
			projectState.LogUntrackedPaths(logger)

			// Diff
			differ := diff.NewDiffer(projectState)
			diffResults, err := differ.Diff()
			if err != nil {
				return err
			}

			// Get plan
			pull := plan.Pull(diffResults)
			pull.LogInfo(root.logger)

			// Dry run?
			if dryRun {
				root.logger.Info("Pull dry run done. Nothing changed.")
				return nil
			}

			// Invoke
			if err := pull.Invoke(root.ctx, manifest, root.api, root.logger); err != nil {
				return err
			}

			// Save manifest
			if err = manifest.Save(); err != nil {
				return err
			}
			root.logger.Debugf("Saved manifest file \"%s\".", utils.RelPath(projectDir, manifest.Path()))

			// Send successful event
			successful = true
			sendPullSuccessfulEvent(root, api)

			// Done
			root.logger.Info("Pull done.")
			return nil
		},
	}

	// Pull command flags
	cmd.Flags().SortFlags = true
	cmd.Flags().BoolVar(&force, "force", false, "ignore invalid local state")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print what needs to be done")

	return cmd
}

func sendPullSuccessfulEvent(root *rootCommand, api *remote.StorageApi) {
	message := "Initialized local project directory."
	duration := time.Since(root.start)
	params := map[string]interface{}{
		"command": "pull",
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
	}
	event, err := api.CreateEvent("info", message, duration, params, results)
	if err == nil {
		root.logger.Debugf("Sent \"pull\" successful event id: \"%s\"", event.Id)
	} else {
		root.logger.Warnf("Cannot send \"pull\" successful event: %s", err)
	}
}

func sendPullFailedEvent(root *rootCommand, api *remote.StorageApi, err error) {
	message := "Pull command failed."
	duration := time.Since(root.start)
	params := map[string]interface{}{
		"command": "pull",
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := api.CreateEvent("error", message, duration, params, results)
	if err == nil {
		root.logger.Debugf("Sent \"pull\" failed event id: \"%s\"", event.Id)
	} else {
		root.logger.Warnf("Cannot send \"pull\" failed event: %s", err)
	}
}
