package cli

import (
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/event"
	"keboola-as-code/src/plan"
	"keboola-as-code/src/remote"
)

const pushShortDescription = `Push configurations to the Keboola Connection project`
const pushLongDescription = `Command "push"

Push configurations to the Keboola Connection project.
Project's state will be overwritten to match the local files.

You can use the "--dry-run" flag to see
what needs to be done without modifying the project's state.
`

func pushCommand(root *rootCommand) *cobra.Command {
	force := false
	cmd := &cobra.Command{
		Use:   "push",
		Short: pushShortDescription,
		Long:  pushLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Define action on diff results
			action := &diffProcessCmd{root: root, cmd: cmd}
			action.onSuccess = func(api *remote.StorageApi) {
				event.SendCmdSuccessfulEvent(root.start, root.logger, api, "push", "Push command done.")
				root.logger.Info("Push done.")
			}
			action.onError = func(api *remote.StorageApi, err error) {
				event.SendCmdFailedEvent(root.start, root.logger, api, err, "push", "Push command failed.")
			}
			action.action = func(api *remote.StorageApi, diffResults *diff.Results) error {
				logger := root.logger
				projectState := diffResults.CurrentState
				projectManifest := projectState.Manifest()

				// Log untracked paths
				projectState.LogUntrackedPaths(logger)

				// Validate schemas
				if err := ValidateSchemas(projectState, logger); err != nil {
					return err
				}

				// Get plan
				push := plan.Push(diffResults)

				// Allow remote deletion, if --force
				if force {
					push.AllowRemoteDelete()
				}

				// Log plan
				push.LogInfo(logger)

				// Dry run?
				dryRun, _ := cmd.Flags().GetBool("dry-run")
				if dryRun {
					logger.Info("Dry run, nothing changed.")
					return nil
				}

				// Invoke
				executor := plan.NewExecutor(logger, root.ctx, projectState, root.api)
				if err := executor.Invoke(push); err != nil {
					return err
				}

				// Save manifest
				if _, err := SaveManifest(projectManifest, logger); err != nil {
					return err
				}

				return nil
			}

			return action.run()
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().BoolVar(&force, "force", false, "enable deleting of remote objects")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
