package table

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
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

			// Ask options
			var tableID keboola.TableID
			var primaryKey []string
			if len(args) < 1 {
				id, createNew, err := askTable(cmd.Context(), d, true)
				if err != nil {
					return err
				}
				tableID = id

				if createNew {
					primaryKey = d.Dialogs().AskPrimaryKey()
				}
			} else {
				id, err := keboola.ParseTableID(args[0])
				if err != nil {
					return err
				}
				tableID = id
			}

			var inputFile string
			if len(args) >= 2 {
				inputFile = args[1]
			}

			fileUploadOpts, err := d.Dialogs().AskUploadFile(inputFile, tableID.String())
			if err != nil {
				return err
			}

			file, err := fileUpload.Run(cmd.Context(), fileUploadOpts, d)
			if err != nil {
				return err
			}

			tableImportOpts := tableImport.Options{
				FileID:          file.ID,
				TableID:         tableID,
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

	uploadFlags := NewUploadFlags()
	_ = cliconfig.GenerateFlags(uploadFlags, cmd.Flags())

	return cmd
}
