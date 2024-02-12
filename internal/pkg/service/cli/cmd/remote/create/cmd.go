package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/bucket"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/table"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	createBranchCmd := branch.Command(p)
	createBucketCmd := bucket.Command(p)
	createTableCmd := table.Command(p)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/create/short`),
		Long:  helpmsg.Read(`remote/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := p.RemoteCommandScope(cmd.Context())
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

	cmd.AddCommand(createBranchCmd)
	cmd.AddCommand(createBucketCmd)
	cmd.AddCommand(createTableCmd)
	return cmd
}
