package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Name            configmap.Value[string] `configKey:"name" configUsage:"name of the workspace"`
	Type            configmap.Value[string] `configKey:"type" configUsage:"type of the workspace"`
	Size            configmap.Value[string] `configKey:"size" configUsage:"size of the workspace"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/workspace/create/short`),
		Long:  helpmsg.Read(`remote/workspace/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Ask options
			options, err := AskCreateWorkspace(d.Dialogs(), f)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-workspace-create")

			// Run operation
			err = create.Run(cmd.Context(), options, d)
			if err != nil {
				return err
			}

			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
