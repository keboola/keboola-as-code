package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

type CreateFlags struct {
	ID             string `mapstructure:"id" usage:"template ID"`
	Name           string `mapstructure:"name" usage:"template name"`
	Description    string `mapstructure:"description" usage:"template description"`
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Branch         string `mapstructure:"branch" shorthand:"b" usage:"branch ID or name"`
	Configs        string `mapstructure:"configs" shorthand:"c" usage:"comma separated list of {componentId}:{configId}"`
	UsedComponents string `mapstructure:"used-components" shorthand:"u" usage:"comma separated list of component ids"`
	AllConfigs     bool   `mapstructure:"all-configs" shorthand:"a" usage:"use all configs from the branch"`
	AllInputs      bool   `mapstructure:"all-inputs" usage:"use all found config/row fields as user inputs"`
}

func CreateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: helpmsg.Read(`template/create/short`),
		Long:  helpmsg.Read(`template/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in template repository
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateTemplateOpts(cmd.Context(), d)
			if err != nil {
				return err
			}

			// Create template
			return createOp.Run(cmd.Context(), options, d)
		},
	}

	cliconfig.MustGenerateFlags(CreateFlags{}, cmd.Flags())

	return cmd
}
