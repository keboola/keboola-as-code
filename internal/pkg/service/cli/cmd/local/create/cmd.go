package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/create/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/create/row"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"master-token" configShorthand:"t" configUsage:"storage API token from your project"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	createConfigCmd := config.Command(p)
	createRowCmd := row.Command(p)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`local/create/short`),
		Long:  helpmsg.Read(`local/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in project directory
			_, d, err := p.LocalProject(cmd.Context(), false, f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreateLocal() {
			case `config`:
				return createConfigCmd.RunE(createConfigCmd, nil)
			case `config row`:
				return createRowCmd.RunE(createRowCmd, nil)
			default:
				// Non-interactive terminal -> print sub-commands.
				return cmd.Help()
			}
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	cmd.AddCommand(createConfigCmd, createRowCmd)
	return cmd
}
