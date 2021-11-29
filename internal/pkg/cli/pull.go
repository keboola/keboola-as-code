package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

const (
	pullShortDescription = `Pull configurations to the project directory`
	pullLongDescription  = `Command "pull"

Pull configurations from the Keboola Connection project.
Local files will be overwritten to match the project's state.

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`
)

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
			action.action = func(api *remote.StorageApi, diffResults *diff.Results) (cmdErr error) {
				logger := root.logger
				projectState := diffResults.CurrentState
				projectManifest := projectState.Manifest()

				// Log untracked paths
				if !force {
					projectState.LogUntrackedPaths(logger)
				}

				// Get plan
				plan, err := pull.NewPlan(diffResults)
				if err != nil {
					return err
				}

				// Log plan
				plan.Log(log.ToInfoWriter(logger))

				// Dry run?
				dryRun := root.options.GetBool("dry-run")
				if dryRun {
					logger.Info("Dry run, nothing changed.")
					logger.Info(`Pull done.`)
					return nil
				}

				// Send cmd successful/failed event
				if eventSender, err := root.GetEventSender(); err == nil {
					defer func() {
						eventSender.SendCmdEvent(root.start, cmdErr, "pull")
					}()
				} else {
					return err
				}

				// Invoke
				if err := plan.Invoke(logger, root.ctx, ``); err != nil {
					return err
				}

				// Normalize paths
				if err := Rename(root.ctx, projectState, logger, false, false); err != nil {
					return err
				}

				// Save manifest
				if _, err := SaveManifest(projectManifest, logger); err != nil {
					return err
				}

				// Validate schemas and encryption
				if err := Validate(projectState, logger, false); err != nil {
					logger.Warn(`Warning, ` + err.Error())
					logger.Warn()
					logger.Warnf(`The project has been pulled, but it is not in a valid state.`)
					logger.Warnf(`Please correct the problems listed above.`)
					logger.Warnf(`Push operation is only possible when project is valid.`)
				}

				logger.Info(`Pull done.`)
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
