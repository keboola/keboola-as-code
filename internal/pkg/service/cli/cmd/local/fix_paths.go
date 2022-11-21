package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func FixPathsCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-paths",
		Short: helpmsg.Read(`local/fix-paths/short`),
		Long:  helpmsg.Read(`local/fix-paths/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := rename.Options{
				DryRun:   d.Options().GetBool(`dry-run`),
				LogEmpty: true,
			}

			// Rename
			_, err = rename.Run(d.CommandCtx(), projectState, options, d)
			return err
		},
	}

	// Flags
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
