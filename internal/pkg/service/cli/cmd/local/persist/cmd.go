// nolint: dupl
package persist

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/persist"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"master-token" configShorthand:"t" configUsage:"storage API token from your project"`
	DryRun          configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "persist",
		Short: helpmsg.Read(`local/persist/short`),
		Long:  helpmsg.Read(`local/persist/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false, f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(cmd.Context(), loadState.PersistOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := persist.Options{
				DryRun:            f.DryRun.Value,
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "local-persist")

			// Persist
			return persist.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
