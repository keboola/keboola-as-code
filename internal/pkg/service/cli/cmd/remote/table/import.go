package table

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	common "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

func ImportCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `import [table] [file]`,
		Short: helpmsg.Read(`remote/table/import/short`),
		Long:  helpmsg.Read(`remote/table/import/long`),
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand(common.WithoutMasterToken())
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-table-import")

			return tableImport.Run(d.CommandCtx(), tableImport.Options{}, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("columns", "", "comma separated list of column names. If present, the first row in the CSV file is not treated as a header")
	cmd.Flags().Bool("incremental-load", false, "data are either added to existing data in the table or replace the existing data")
	cmd.Flags().Bool("without-headers", false, "states if the CSV file contains headers on the first row or not")

	return cmd
}
