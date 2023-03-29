package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func TableCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table [table]",
		Short: helpmsg.Read(`remote/create/table/short`),
		Long:  helpmsg.Read(`remote/create/table/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			_, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP("storage-api-host", "H", "", "if command is run outside the project directory")
	cmd.Flags().String("bucket", "", "bucket ID (required if the tableId argument is empty)")
	cmd.Flags().String("name", "", "name of the table (required if the tableId argument is empty)")
	cmd.Flags().String("columns", "", "comma-separated list of column names")
	cmd.Flags().String("primary-key", "", "columns used as primary key, comma-separated")

	return cmd
}
