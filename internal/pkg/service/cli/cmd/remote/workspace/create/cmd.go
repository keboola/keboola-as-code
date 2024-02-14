package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Name           configmap.Value[string] `configKey:"name" configUsage:"name of the workspace"`
	Type           configmap.Value[string] `configKey:"type" configUsage:"type of the workspace"`
	Size           configmap.Value[string] `configKey:"size" configUsage:"size of the workspace"`
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

			flags := Flags{}
			err = configmap.Bind(configmap.BindConfig{
				Flags:     cmd.Flags(),
				Args:      args,
				EnvNaming: env.NewNamingConvention("KBC_"),
				Envs:      env.Empty(),
			}, &flags)
			if err != nil {
				return err
			}

			//err = configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
			//	Args:                   args,
			//	EnvNaming:              env.NewNamingConvention("KBC_"),
			//	Envs:                   env.Empty(),
			//	GenerateHelpFlag:       true,
			//	GenerateConfigFileFlag: true,
			//	GenerateDumpConfigFlag: true,
			//}, &flags)

			// Ask options
			options, err := AskCreateWorkspace(flags, d.Dialogs())
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

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
