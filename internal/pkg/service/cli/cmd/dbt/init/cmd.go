package init

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

type Flags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" shorthand:"T" usage:"target name of the profile"`
	WorkspaceName  string `mapstructure:"workspace-name" shorthand:"W" usage:"name of workspace to create"`
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

			// Ask options
			opts, err := p.BaseScope().Dialogs().AskDbtInit()
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			return initOp.Run(cmd.Context(), opts, d)
		},
	}

	cliconfig.MustGenerateFlags(Flags{}, cmd.Flags())

	return cmd
}
