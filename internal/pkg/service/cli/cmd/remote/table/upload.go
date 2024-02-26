package table

import (
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	fileUpload "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

func UploadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [table] [file]`,
		Short: helpmsg.Read(`remote/table/upload/short`),
		Long:  helpmsg.Read(`remote/table/upload/long`),
		Args:  cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Get default branch
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(cmd.Context())
			if err != nil {
				return errors.Errorf("cannot get default branch: %w", err)
			}

			// Ask options
			tableKey := keboola.TableKey{BranchID: branch.ID}
			var primaryKey []string
			if len(args) < 1 {
				key, createNew, err := askTable(cmd.Context(), d, branch.ID, true)
				if err != nil {
					return err
				}
				tableKey = key
				if createNew {
					primaryKey = d.Dialogs().AskPrimaryKey()
				}
			} else if id, err := keboola.ParseTableID(args[0]); err == nil {
				tableKey.TableID = id
			} else {
				return err
			}

			var inputFile string
			if len(args) >= 2 {
				inputFile = args[1]
			}

			fileName := fmt.Sprintf("table-upload-%s", tableKey.TableID.String())
			fileUploadOpts, err := d.Dialogs().AskUploadFile(branch.BranchKey, inputFile, fileName)
			if err != nil {
				return err
			}

			file, err := fileUpload.Run(cmd.Context(), fileUploadOpts, d)
			if err != nil {
				return err
			}

			tableImportOpts := tableImport.Options{
				FileKey:         file.FileKey,
				TableKey:        tableKey,
				Columns:         d.Options().GetStringSlice("columns"),
				Delimiter:       d.Options().GetString("file-delimiter"),
				Enclosure:       d.Options().GetString("file-enclosure"),
				EscapedBy:       d.Options().GetString("file-escaped-by"),
				IncrementalLoad: d.Options().GetBool("incremental-load"),
				WithoutHeaders:  d.Options().GetBool("file-without-headers"),
				PrimaryKey:      primaryKey,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-upload")

			return tableImport.Run(cmd.Context(), tableImportOpts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("columns", "", "comma separated list of column names. If present, the first row in the CSV file is not treated as a header")
	cmd.Flags().Bool("incremental-load", false, "data are either added to existing data in the table or replace the existing data")
	cmd.Flags().Bool("file-without-headers", false, "states if the CSV file contains headers on the first row or not")
	cmd.Flags().StringSlice("primary-key", nil, "primary key for the newly created table if the table doesn't exist")
	cmd.Flags().String("file-name", "", "name of the file to be created")
	cmd.Flags().String("file-tags", "", "comma-separated list of file tags")
	cmd.Flags().String("file-delimiter", ",", "field delimiter used in the CSV file")
	cmd.Flags().String("file-enclosure", `"`, "field enclosure used in the CSV file")
	cmd.Flags().String("file-escaped-by", "", "escape character used in the CSV file")

	return cmd
}
