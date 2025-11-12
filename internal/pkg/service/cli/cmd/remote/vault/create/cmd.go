package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Name            configmap.Value[string] `configKey:"name" configUsage:"name of the vault variable"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `create [name]`,
		Short: helpmsg.Read(`remote/vault/create/short`),
		Long:  helpmsg.Read(`remote/vault/create/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			logger := d.Logger()

			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			var variableName string
			if len(args) > 0 {
				variableName = args[0]
			} else if f.Name.Value != "" {
				variableName = f.Name.Value
			} else {
				variableName, _ = d.Dialogs().Ask(AskVariableName())
			}

			variableValue, _ := d.Dialogs().Ask(AskVariableValue())

			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-vault-create")

			logger.Infof(cmd.Context(), "Creating vault variable \"%s\"...", variableName)

			payload := &keboola.VaultVariableCreatePayload{
				Key:   variableName,
				Value: variableValue,
			}

			variable, err := d.KeboolaProjectAPI().CreateVariableRequest(payload).Send(cmd.Context())
			if err != nil {
				return errors.Errorf("failed to create vault variable: %w", err)
			}

			logger.Infof(cmd.Context(), "Vault variable \"%s\" created with hash: %s", variableName, variable.Hash)

			prj.ProjectManifest().AddVaultVariable(variable)

			if _, err := saveManifest.Run(cmd.Context(), prj.ProjectManifest(), prj.Fs(), d); err != nil {
				return errors.Errorf("failed to save manifest: %w", err)
			}

			logger.Info(cmd.Context(), "Manifest updated successfully.")

			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
