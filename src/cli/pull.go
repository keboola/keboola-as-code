package cli

import (
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/event"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/plan"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

const pullShortDescription = `Pull configurations to the local project dir`
const pullLongDescription = `Command "pull"

Pull configurations from the Keboola Connection project.
Local files will be overwritten to match the project's state.

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`

func pullCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: pullShortDescription,
		Long:  pullLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Ask for the host/token, if not specified
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiHost", "ApiToken"}); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			action := &diffProcessCmd{root: root, cmd: cmd}
			action.onSuccess = func(api *remote.StorageApi) {
				event.SendCmdSuccessfulEvent(root.start, root.logger, api, "pull", "Pull command done.")
				root.logger.Info("Pull done.")
			}
			action.onError = func(api *remote.StorageApi, err error) {
				event.SendCmdFailedEvent(root.start, root.logger, api, err, "pull", "Pull command failed.")
			}
			action.action = func(api *remote.StorageApi, projectManifest *manifest.Manifest, projectState *state.State, diffResults *diff.Results) error {
				// Log untracked paths
				projectState.LogUntrackedPaths(root.logger)

				// Get plan
				pull := plan.Pull(diffResults)
				pull.LogInfo(root.logger)

				// Dry run?
				dryRun, _ := cmd.Flags().GetBool("dry-run")
				if dryRun {
					root.logger.Info("Dry run, nothing changed.")
					return nil
				}

				// Invoke
				executor := plan.NewExecutor(root.logger, root.ctx, root.api, projectManifest)
				if err := executor.Invoke(pull); err != nil {
					return err
				}

				// Save manifest
				if projectManifest.IsChanged() {
					if err = projectManifest.Save(); err != nil {
						return err
					}
					root.logger.Debugf("Saved manifest file \"%s\".", utils.RelPath(projectManifest.ProjectDir, projectManifest.Path()))
				}

				return nil
			}

			return action.run()
		},
	}

	// Pull command flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
