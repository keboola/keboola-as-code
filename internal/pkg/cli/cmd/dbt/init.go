package dbt

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

func InitCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `init`,
		Short: helpmsg.Read(`dbt/init/short`),
		Long:  helpmsg.Read(`dbt/init/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Options
			opts, err := d.Dialogs().AskDbtInit(d)
			if err != nil {
				return err
			}

			return initOp.Run(d.CommandCtx(), opts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("target-name", "T", "", "target name of the profile")
	cmd.Flags().StringP("workspace-name", "W", "", "name of workspace to create")

	return cmd
}
