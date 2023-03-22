package table

import (
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	common "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

func ImportCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `import [table] [file]`,
		Short: helpmsg.Read(`remote/table/import/short`),
		Long:  helpmsg.Read(`remote/table/import/long`),
		Args:  cobra.MaximumNArgs(2),
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

			var tableID keboola.TableID
			var primaryKey []string
			if len(args) < 1 {
				allTables, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithColumns()).Send(d.CommandCtx())
				if err != nil {
					return err
				}

				table, err := d.Dialogs().AskTable(d.Options(), *allTables, dialog.WithAllowCreateNewTable())
				if err != nil {
					return err
				}

				if table != nil {
					// user selected table
					tableID = table.ID
				} else {
					// user asked to create new table
					tableID, err = keboola.ParseTableID(d.Dialogs().AskTableID())
					if err != nil {
						return err
					}

					primaryKey = d.Dialogs().AskPrimaryKey(d.Options())
				}
			} else {
				id, err := keboola.ParseTableID(args[0])
				if err != nil {
					return err
				}
				tableID = id
			}

			var fileID int
			if len(args) < 2 {
				allRecentFiles, err := d.KeboolaProjectAPI().ListFilesRequest().Send(d.CommandCtx())
				if err != nil {
					return err
				}
				file, err := d.Dialogs().AskFile(*allRecentFiles)
				if err != nil {
					return err
				}
				fileID = file.ID
			} else {
				fileID, err = strconv.Atoi(args[1])
				if err != nil {
					return err
				}
			}

			opts := tableImport.Options{
				FileID:          fileID,
				TableID:         tableID,
				Columns:         d.Options().GetStringSlice("columns"),
				IncrementalLoad: d.Options().GetBool("incremental-load"),
				WithoutHeaders:  d.Options().GetBool("without-headers"),
				PrimaryKey:      primaryKey,
			}

			return tableImport.Run(d.CommandCtx(), opts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("columns", "", "comma separated list of column names. If present, the first row in the CSV file is not treated as a header")
	cmd.Flags().Bool("incremental-load", false, "data are either added to existing data in the table or replace the existing data")
	cmd.Flags().Bool("without-headers", false, "states if the CSV file contains headers on the first row or not")
	cmd.Flags().StringSlice("primary-key", nil, "primary key for the newly created table if the table doesn't exist")

	return cmd
}
