package cli

import (
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/remote"
)

const diffShortDescription = `Print differences between local and remote state`
const diffLongDescription = `Command "diff"

Print differences between local and remote state.
`

func diffCommand(root *rootCommand) *cobra.Command {
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

				// Explain
				root.logger.Info("Description:")
				root.logger.Info("\tCH changed")
				root.logger.Info("\t+  only in the remote state")
				root.logger.Info("\t-  only in the local state")
				root.logger.Info("")

				// Print diff
				root.logger.Info("Diff:")
				differencesCount := 0
				for _, result := range diffResults.Results {
					if result.State != diff.ResultEqual {
						root.logger.Infof("%s %s %s", result.Mark(), result.Kind().Abbr, result.RelativePath())
						differencesCount++
					}
				}

				// No differences?
				if differencesCount == 0 {
					root.logger.Info("No differences.")
				}

				return nil
			}
			return action.run()
		},
	}

	return cmd
}
