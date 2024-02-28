package list

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/list"
)

type Flags struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `list`,
		Short: helpmsg.Read(`remote/workspace/list/short`),
		Long:  helpmsg.Read(`remote/workspace/list/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Ask options
			err = list.Run(cmd.Context(), d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-list-workspace")

			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
