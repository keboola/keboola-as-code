package list

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/list"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: helpmsg.Read(`template/list/short`),
		Long:  helpmsg.Read(`template/list/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in template repository
			repo, d, err := p.LocalRepository(cmd.Context(), f.StorageAPIHost, dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			// Describe template
			return listOp.Run(cmd.Context(), repo, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
	return cmd
}
