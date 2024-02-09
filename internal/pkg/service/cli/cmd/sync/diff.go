package sync

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/printdiff"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type DiffFlag struct {
	Details bool `mapstructure:"details" usage:"print changed fields"`
}

func DiffCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: helpmsg.Read(`sync/diff/short`),
		Long:  helpmsg.Read(`sync/diff/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			_, _, err := p.BaseScope().FsInfo().ProjectDir(cmd.Context())
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Get local project
			prj, _, err := d.LocalProject(cmd.Context(), false)
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
			results, err := printdiff.Run(cmd.Context(), projectState, options, d)
			if err != nil {
				return err
			}

			// Print info about --details flag
			if !options.PrintDetails && results.HasNotEqualResult {
				logger := d.Logger()
				logger.Info(cmd.Context(), "")
				logger.Info(cmd.Context(), `Use --details flag to list the changed fields.`)
			}
			return nil
		},
	}

	cliconfig.MustGenerateFlags(DiffFlag{}, cmd.Flags())

	return cmd
}
