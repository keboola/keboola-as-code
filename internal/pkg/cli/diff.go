package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

const (
	diffShortDescription = `Print differences between local and remote state`
	diffLongDescription  = `Command "diff"

Print differences between local and remote state.
`
)

func diffCommand(root *rootCommand) *cobra.Command {
	printDetails := false
	cmd := &cobra.Command{
		Use:   "diff",
		Short: diffShortDescription,
		Long:  diffLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			action := &diffProcessCmd{root: root, cmd: cmd}
			action.action = func(api *remote.StorageApi, diffResults *diff.Results) error {
				state := diffResults.CurrentState

				// Log untracked paths
				state.LogUntrackedPaths(root.logger)

				if diffResults.Equal {
					root.logger.Info("No difference.")
				} else {
					// Explain
					root.logger.Info(diff.ChangeMark + " changed")
					root.logger.Info(diff.OnlyInRemoteMark + " remote state")
					root.logger.Info(diff.OnlyInLocalMark + " local state")
					root.logger.Info("")

					// Print diff
					root.logger.Info("Diff:")
					for _, line := range diffResults.Format(printDetails) {
						root.logger.Info(line)
					}
				}

				return nil
			}
			return action.run()
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().BoolVar(&printDetails, "details", false, "print changed fields")

	return cmd
}
