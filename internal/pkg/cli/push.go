package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

const pushShortDescription = `Push configurations to the Keboola Connection project`
const pushLongDescription = `Command "push"

Push configurations to the Keboola Connection project.
Project's state will be overwritten to match the local files.

You can specify an optional ["change description"].
It will be visible in the config's versions.

You can use the "--dry-run" flag to see
what needs to be done without modifying the project's state.
`

func pushCommand(root *rootCommand) *cobra.Command {
	force := false
	cmd := &cobra.Command{
		Use:   `push ["change description"]`,
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
				encrypt := root.options.GetBool("encrypt")

				// Change description - optional arg
				changeDescription := "Updated from #KeboolaCLI"
				if len(args) > 0 {
					changeDescription = args[0]
				}
				logger.Debugf(`Change description: "%s"`, changeDescription)

				// Log untracked paths
				projectState.LogUntrackedPaths(logger)

				// Validate schemas and encryption
				if err := Validate(projectState, logger, encrypt); err != nil {
					return err
				}

				// Get plan
				push, err := plan.Push(diffResults, changeDescription)
				if err != nil {
					return err
				}

				// Get encrypt plan
				encryptPlan := plan.Encrypt(projectState)

				// Allow remote deletion, if --force
				if force {
					push.AllowRemoteDelete()
				}

				// Encrypt plan
				if encrypt {
					encryptPlan.Log(log.ToInfoWriter(logger))
				}

				// Log plan
				push.Log(log.ToInfoWriter(logger))

				// Dry run?
				dryRun := root.options.GetBool("dry-run")
				if dryRun {
					logger.Info("Dry run, nothing changed.")
					return nil
				}

				if encrypt {
					// Get encryption API
					encryptionApiUrl, err := api.GetEncryptionApiUrl()
					if err != nil {
						return err
					}

					// Invoke
					encryptionApi := encryption.NewEncryptionApi(encryptionApiUrl, root.ctx, logger, false)
					if err := encryptPlan.Invoke(api.ProjectId(), logger, encryptionApi, projectState); err != nil {
						return err
					}
				}

				// Invoke
				if err := push.Invoke(logger, api, root.ctx); err != nil {
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
	cmd.Flags().Bool("encrypt", false, "encrypt unencrypted values before push")
	return cmd
}
