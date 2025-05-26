package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/bucket"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Commands(p dependencies.Provider) *cobra.Command {
	createBranchCmd := branch.Command(p)
	createBucketCmd := bucket.Command(p)
	createTableCmd := table.Command(p)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/create/short`),
		Long:  helpmsg.Read(`remote/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreateRemote() {
			case `branch`:
				return createBranchCmd.RunE(createBranchCmd, nil)
			case `bucket`:
				return createBucketCmd.RunE(createBucketCmd, nil)
			case `table`:
				return createTableCmd.RunE(createTableCmd, nil)
			default:
				// Non-interactive terminal -> print sub-commands.
				return cmd.Help()
			}
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	cmd.AddCommand(createBranchCmd)
	cmd.AddCommand(createBucketCmd)
	cmd.AddCommand(createTableCmd)
	return cmd
}
