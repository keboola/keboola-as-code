package deletevault

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
	Name            configmap.Value[string] `configKey:"name" configUsage:"name of the vault variable to delete"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete [name]`,
		Short: helpmsg.Read(`remote/vault/delete/short`),
		Long:  helpmsg.Read(`remote/vault/delete/long`),
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

			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-vault-delete")

			logger.Infof(cmd.Context(), "Fetching vault variables...")

			allVariables, err := d.KeboolaProjectAPI().ListVariablesRequest(nil).Send(cmd.Context())
			if err != nil {
				return errors.Errorf("failed to list vault variables: %w", err)
			}

			var targetVariable *keboola.VaultVariable
			for _, v := range *allVariables {
				if v.Key == variableName {
					targetVariable = v
					break
				}
			}

			if targetVariable == nil {
				return errors.Errorf("vault variable \"%s\" not found", variableName)
			}

			logger.Infof(cmd.Context(), "Deleting vault variable \"%s\" (hash: %s)...", variableName, targetVariable.Hash)

			if _, err := d.KeboolaProjectAPI().DeleteVariableRequest(targetVariable.Hash).Send(cmd.Context()); err != nil {
				return errors.Errorf("failed to delete vault variable: %w", err)
			}

			logger.Infof(cmd.Context(), "Vault variable \"%s\" deleted successfully.", variableName)

			if prj.ProjectManifest().RemoveVaultVariable(targetVariable.Hash) {
				if _, err := saveManifest.Run(cmd.Context(), prj.ProjectManifest(), prj.Fs(), d); err != nil {
					return errors.Errorf("failed to save manifest: %w", err)
				}
				logger.Info(cmd.Context(), "Manifest updated successfully.")
			} else {
				logger.Info(cmd.Context(), "Variable was not in manifest.")
			}

			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
