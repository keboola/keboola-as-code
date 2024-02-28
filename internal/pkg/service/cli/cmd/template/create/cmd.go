package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

type Flags struct {
	ID             configmap.Value[string] `configKey:"id" configUsage:"template ID"`
	Name           configmap.Value[string] `configKey:"name" configUsage:"template name"`
	Description    configmap.Value[string] `configKey:"description" configUsage:"template description"`
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Branch         configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Configs        configmap.Value[string] `configKey:"configs" configShorthand:"c" configUsage:"comma separated list of {componentId}:{configId}"`
	UsedComponents configmap.Value[string] `configKey:"used-components" configShorthand:"u" configUsage:"comma separated list of component ids"`
	AllConfigs     configmap.Value[bool]   `configKey:"all-configs" configShorthand:"a" configUsage:"use all configs from the branch"`
	AllInputs      configmap.Value[bool]   `configKey:"all-inputs" configUsage:"use all found config/row fields as user inputs"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: helpmsg.Read(`template/create/short`),
		Long:  helpmsg.Read(`template/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in template repository
			dep, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			f := Flags{}
			if err = p.BaseScope().ConfigBinder().Bind(cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Options
			options, err := AskCreateTemplateOpts(cmd.Context(), dep.Dialogs(), dep, f)
			if err != nil {
				return err
			}

			// Create template
			return createOp.Run(cmd.Context(), options, dep)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
