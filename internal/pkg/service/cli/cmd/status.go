package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/status"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
}

func DefaultFlags() Flags {
	return Flags{}
}

func StatusCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: helpmsg.Read(`status/short`),
		Long:  helpmsg.Read(`status/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			d, err := p.LocalCommandScope(cmd.Context(), f.StorageAPIHost, dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			return status.Run(cmd.Context(), d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
