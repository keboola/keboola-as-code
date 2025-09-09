package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"master-token" configShorthand:"t" configUsage:"storage API token from your project"`
	ID              configmap.Value[string] `configKey:"id" configUsage:"template ID"`
	Name            configmap.Value[string] `configKey:"name" configUsage:"template name"`
	Description     configmap.Value[string] `configKey:"description" configUsage:"template description"`
	Branch          configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Configs         configmap.Value[string] `configKey:"configs" configShorthand:"c" configUsage:"comma separated list of {componentId}:{configId}"`
	UsedComponents  configmap.Value[string] `configKey:"used-components" configShorthand:"u" configUsage:"comma separated list of component ids"`
	AllConfigs      configmap.Value[bool]   `configKey:"all-configs" configShorthand:"a" configUsage:"use all configs from the branch"`
	AllInputs       configmap.Value[bool]   `configKey:"all-inputs" configUsage:"use all found config/row fields as user inputs"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: helpmsg.Read(`template/create/short`),
		Long:  helpmsg.Read(`template/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in template repository
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Options
			options, err := AskCreateTemplateOpts(cmd.Context(), d.Dialogs(), d, f)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "template-create")

			// Create template
			return createOp.Run(cmd.Context(), options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
