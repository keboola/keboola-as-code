// nolint: dupl
package encrypt

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flag struct {
	DryRun configmap.Value[bool] `configKey:"dry-run" configUsage:"print what needs to be done"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "encrypt",
		Short: helpmsg.Read(`local/encrypt/short`),
		Long:  helpmsg.Read(`local/encrypt/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			f := Flag{}
			if err = p.BaseScope().ConfigBinder().Bind(cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := encrypt.Options{
				DryRun:   f.DryRun.Value,
				LogEmpty: true,
			}

			// Encrypt
			return encrypt.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flag{})

	return cmd
}
