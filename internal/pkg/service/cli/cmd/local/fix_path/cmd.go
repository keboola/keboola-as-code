package fix_path

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flag struct {
	DryRun bool `configKey:"dry-run" configUsage:"print what needs to be done"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-paths",
		Short: helpmsg.Read(`local/fix-paths/short`),
		Long:  helpmsg.Read(`local/fix-paths/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
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
			_, err = rename.Run(cmd.Context(), projectState, options, d)
			return err
		},
	}

	cliconfig.MustGenerateFlags(Flag{}, cmd.Flags())

	return cmd
}
