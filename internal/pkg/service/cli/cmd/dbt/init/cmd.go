package init

import (
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     configmap.Value[string] `configKey:"target-name" configShorthand:"T" configUsage:"target name of the profile"`
	WorkspaceName  configmap.Value[string] `configKey:"workspace-name" configShorthand:"W" configUsage:"name of workspace to create"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `init`,
		Short: helpmsg.Read(`dbt/init/short`),
		Long:  helpmsg.Read(`dbt/init/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check that we are in dbt directory
			if _, _, err := p.LocalDbtProject(cmd.Context()); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Println("ERROR")
			flags := Flags{}
			err = configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
				Args:                   args,
				EnvNaming:              env.NewNamingConvention("MY_APP_"),
				Envs:                   env.Empty(),
				GenerateHelpFlag:       true,
				GenerateConfigFileFlag: true,
				GenerateDumpConfigFlag: true,
			}, &flags)
			if err != nil {
				return err
			}

			// Ask options
			opts, err := AskDbtInit(flags, d.Dialogs())
			if err != nil {
				return err
			}

			return initOp.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
