package fixpath

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flag struct {
	DryRun configmap.Value[bool] `configKey:"dry-run" configUsage:"print what needs to be done"`
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

			// flags
			f := Flag{}
			if err = configmap.Bind(utils.GetBindConfig(cmd.Flags(), args), &f); err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := rename.Options{
				DryRun:   f.DryRun.Value,
				LogEmpty: true,
			}

			// Rename
			_, err = rename.Run(cmd.Context(), projectState, options, d)
			return err
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flag{})

	return cmd
}
