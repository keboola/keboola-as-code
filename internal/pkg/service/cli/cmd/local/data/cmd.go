// Package data implements the "kbc local data" command.
package data

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/data"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

// Flags for the local data command.
type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Limit           configmap.Value[uint]   `configKey:"limit" configUsage:"maximum number of rows to download per table (0 = unlimited)"`
}

// DefaultFlags returns the default flag values.
func DefaultFlags() Flags {
	return Flags{
		Limit: configmap.NewValue(uint(data.DefaultRowLimit)),
	}
}

// Command returns the "kbc local data" command.
func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "data <config-path>",
		Short: helpmsg.Read(`local/data/short`),
		Long:  helpmsg.Read(`local/data/long`),
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Parse flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get config path from arguments
			configPath := args[0]

			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false, f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(cmd.Context(), loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := data.Options{
				ConfigPath: configPath,
				RowLimit:   f.Limit.Value,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "local-data")

			// Run the data download
			return data.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
