package table

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	common "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	fileUpload "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

func UploadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [file] [table]`,
		Short: helpmsg.Read(`remote/table/upload/short`),
		Long:  helpmsg.Read(`remote/table/upload/long`),
		Args:  cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			var inputFile string
			if len(args) > 0 {
				inputFile = args[0]
			}

			fileUploadOpts, err := baseDeps.Dialogs().AskUploadFile(baseDeps.Options(), inputFile)
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand(common.WithoutMasterToken())
			if err != nil {
				return err
			}

			var tableID keboola.TableID
			var primaryKey []string
			if len(args) < 2 {
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
				id, err := keboola.ParseTableID(args[1])
				if err != nil {
					return err
				}
				tableID = id
			}

			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-table-upload")

			file, err := fileUpload.Run(d.CommandCtx(), fileUploadOpts, d)
			if err != nil {
				return err
			}

			tableImportOpts := tableImport.Options{
				FileID:          file.ID,
				TableID:         tableID,
				Columns:         d.Options().GetStringSlice("columns"),
				IncrementalLoad: d.Options().GetBool("incremental-load"),
				WithoutHeaders:  d.Options().GetBool("without-headers"),
				PrimaryKey:      primaryKey,
			}

			return tableImport.Run(d.CommandCtx(), tableImportOpts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("columns", "", "comma separated list of column names. If present, the first row in the CSV file is not treated as a header")
	cmd.Flags().Bool("incremental-load", false, "data are either added to existing data in the table or replace the existing data")
	cmd.Flags().Bool("without-headers", false, "states if the CSV file contains headers on the first row or not")
	cmd.Flags().StringSlice("primary-key", nil, "primary key for the newly created table if the table doesn't exist")
	cmd.Flags().String("name", "", "name of the file to be created")
	cmd.Flags().String("tags", "", "comma-separated list of tags")

	return cmd
}
