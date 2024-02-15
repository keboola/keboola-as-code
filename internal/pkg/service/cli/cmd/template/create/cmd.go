package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

type Flags struct {
	ID             string `configKey:"id" configUsage:"template ID"`
	Name           string `configKey:"name" configUsage:"template name"`
	Description    string `configKey:"description" configUsage:"template description"`
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Branch         string `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Configs        string `configKey:"configs" configShorthand:"c" configUsage:"comma separated list of {componentId}:{configId}"`
	UsedComponents string `configKey:"used-components" configShorthand:"u" configUsage:"comma separated list of component ids"`
	AllConfigs     bool   `configKey:"all-configs" configShorthand:"a" configUsage:"use all configs from the branch"`
	AllInputs      bool   `configKey:"all-inputs" configUsage:"use all found config/row fields as user inputs"`
}

func Command(p dependencies.Provider) *cobra.Command {
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

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
