package cli

import (
	"github.com/spf13/cobra"

	"keboola-as-code/src/diff"
	"keboola-as-code/src/event"
	"keboola-as-code/src/log"
	"keboola-as-code/src/plan"
	"keboola-as-code/src/remote"
)

const pullShortDescription = `Pull configurations to the project directory`
const pullLongDescription = `Command "pull"

Pull configurations from the Keboola Connection project.
Local files will be overwritten to match the project's state.

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`

func pullCommand(root *rootCommand) *cobra.Command {
	force := false
	cmd := &cobra.Command{
		Use:   "pull",
		Short: pullShortDescription,
		Long:  pullLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Define action on diff results
			action := &diffProcessCmd{root: root, cmd: cmd}
			action.invalidStateCanBeIgnored = true
			action.ignoreInvalidState = force
			action.onSuccess = func(api *remote.StorageApi) {
				event.SendCmdSuccessfulEvent(root.start, root.logger, api, "pull", "Pull command done.")
				root.logger.Info("Pull done.")
			}
			action.onError = func(api *remote.StorageApi, err error) {
				event.SendCmdFailedEvent(root.start, root.logger, api, err, "pull", "Pull command failed.")
			}
			action.action = func(api *remote.StorageApi, diffResults *diff.Results) error {
				logger := root.logger
				projectState := diffResults.CurrentState
				projectManifest := projectState.Manifest()

				// Log untracked paths
				projectState.LogUntrackedPaths(logger)

				// Get plan
				pull, err := plan.Pull(diffResults)
				if err != nil {
					return err
				}

				// Log plan
				pull.Log(log.ToInfoWriter(logger))

				// Dry run?
				dryRun := root.options.GetBool("dry-run")
				if dryRun {
					logger.Info("Dry run, nothing changed.")
					return nil
				}

				// Invoke
				if err := pull.Invoke(logger, root.api, root.ctx); err != nil {
					return err
				}

				// Normalize paths
				if err := Rename(projectState, logger, false, false); err != nil {
					return err
				}

				// Save manifest
				if _, err := SaveManifest(projectManifest, logger); err != nil {
					return err
				}

				// Validate schemas and encryption
				if err := Validate(projectState, logger); err != nil {
					logger.Warn(`Warning, ` + err.Error())
					logger.Warn()
					logger.Warnf(`The project has been pulled, but it is not in a valid state.`)
					logger.Warnf(`Please correct the problems listed above.`)
					logger.Warnf(`Push operation is only possible when project is valid.`)
				}

				return nil
			}

			return action.run()
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().BoolVar(&force, "force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
