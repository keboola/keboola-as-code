package create

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

type Flags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Name           string `mapstructure:"name" usage:"name of the workspace"`
	Type           string `mapstructure:"type" usage:"type of the workspace"`
	Size           string `mapstructure:"size" usage:"size of the workspace"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/workspace/create/short`),
		Long:  helpmsg.Read(`remote/workspace/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Ask options
			options, err := d.Dialogs().AskCreateWorkspace()
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-workspace")

			// Run operation
			err = create.Run(cmd.Context(), options, d)
			if err != nil {
				return err
			}

			return nil
		},
	}

	createCommandFlags := Flags{}
	cliconfig.MustGenerateFlags(createCommandFlags, cmd.Flags())

	return cmd
}
