// nolint: dupl
package persist

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/persist"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flag struct {
	DryRun bool `configKey:"dry-run" configUsage:"print what needs to be done"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "persist",
		Short: helpmsg.Read(`local/persist/short`),
		Long:  helpmsg.Read(`local/persist/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			flag := Flag{}
			err = configmap.Bind(configmap.BindConfig{
				Flags:     cmd.Flags(),
				Args:      args,
				EnvNaming: env.NewNamingConvention("KBC_"),
				Envs:      env.Empty(),
			}, &flag)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.PersistOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := persist.Options{
				DryRun:            flag.DryRun,
				LogUntrackedPaths: true,
			}
			// Persist
			return persist.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flag{})

	return cmd
}
