package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"keboola-as-code/src/diff"
	"keboola-as-code/src/remote"
)

const diffShortDescription = `Print differences between local and remote state`
const diffLongDescription = `Command "diff"

Print differences between local and remote state.
`

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
					root.logger.Info("CH changed")
					root.logger.Info(diff.OnlyInRemoteMark + "  remote state")
					root.logger.Info(diff.OnlyInLocalMark + "  local state")
					root.logger.Info("")

					// Print diff
					root.logger.Info("Diff:")
					for _, result := range diffResults.Results {
						if result.State != diff.ResultEqual {
							// Message
							msg := fmt.Sprintf("%s %s %s", result.Mark(), result.Kind().Abbr, result.RelativePath())
							if !printDetails && len(result.ChangedFields) > 0 {
								msg += " | changed: " + strings.Join(result.ChangedFields, ", ")
							}
							root.logger.Infof(msg)

							// Changed fields
							if printDetails {
								for field, change := range result.Differences {
									root.logger.Infof("\t\"%s\":", field)
									for _, line := range strings.Split(change, "\n") {
										root.logger.Infof("\t%s", line)
									}
								}
							}
						}
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
