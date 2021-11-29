package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/push"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

const (
	pushShortDescription = `Push configurations to the Keboola Connection project`
	pushLongDescription  = `Command "push"

Push configurations to the Keboola Connection project.
Project's state will be overwritten to match the local files.

You can specify an optional ["change description"].
It will be visible in the config's versions.

You can use the "--dry-run" flag to see
what needs to be done without modifying the project's state.
`
)

func pushCommand(root *rootCommand) *cobra.Command {
	force := false
	cmd := &cobra.Command{
		Use:   `push ["change description"]`,
		Short: pushShortDescription,
		Long:  pushLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Run the encrypt command first, if the --encrypt flag is used
			if root.options.GetBool("encrypt") {
				encryptCmd := root.GetCommandByName("encrypt")
				if err := encryptCmd.RunE(encryptCmd, nil); err != nil {
					return err
				}
			}
			// Define action on diff results
			action := &diffProcessCmd{root: root, cmd: cmd}
			action.action = func(api *remote.StorageApi, diffResults *diff.Results) (cmdErr error) {
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
				plan, err := push.NewPlan(diffResults)
				if err != nil {
					return err
				}

				// Allow remote deletion, if --force
				if force {
					plan.AllowRemoteDelete()
				}

				// Log plan
				plan.Log(log.ToInfoWriter(logger))

				// Dry run?
				dryRun := root.options.GetBool("dry-run")
				if dryRun {
					logger.Info("Dry run, nothing changed.")
					logger.Info(`Push done.`)
					return nil
				}

				// Send cmd successful/failed event
				if eventSender, err := root.GetEventSender(); err == nil {
					defer func() {
						eventSender.SendCmdEvent(root.start, cmdErr, "push")
					}()
				} else {
					return err
				}

				// Invoke
				if err := plan.Invoke(logger, root.ctx, changeDescription); err != nil {
					return err
				}

				// Save manifest
				if _, err := SaveManifest(projectManifest, logger); err != nil {
					return err
				}

				logger.Info(`Push done.`)
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
