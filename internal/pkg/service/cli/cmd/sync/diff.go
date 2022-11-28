package sync

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/printdiff"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func DiffCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: helpmsg.Read(`sync/diff/short`),
		Long:  helpmsg.Read(`sync/diff/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.DiffOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := printdiff.Options{
				PrintDetails:      d.Options().GetBool(`details`),
				LogUntrackedPaths: true,
			}

			// Print diff
			results, err := printdiff.Run(d.CommandCtx(), projectState, options, d)
			if err != nil {
				return err
			}

			// Print info about --details flag
			if !options.PrintDetails && results.HasNotEqualResult {
				logger := d.Logger()
				logger.Info()
				logger.Info(`Use --details flag to list the changed fields.`)
			}
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("details", false, "print changed fields")

	return cmd
}
