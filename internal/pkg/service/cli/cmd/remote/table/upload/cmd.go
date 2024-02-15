package upload

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type Flags struct {
	StorageAPIHost    string   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Columns           string   `configKey:"columns" configUsage:"comma separated list of column names. If present, the first row in the CSV file is not treated as a header"`
	IncrementalLoad   bool     `configKey:"incremental-load" configUsage:"data are either added to existing data in the table or replace the existing data"`
	FileWithoutHeader bool     `configKey:"file-without-headers" configUsage:"states if the CSV file contains headers on the first row or not"`
	PrimaryKeys       []string `configKey:"primary-key" configUsage:"primary key for the newly created table if the table doesn't exist"`
	FileName          string   `configKey:"file-name" configUsage:"name of the file to be created"`
	FileTags          string   `configKey:"file-tags" configUsage:"comma-separated list of file tags"`
	FileDelimiter     string   `configKey:"file-delimiter" configUsage:"field delimiter used in the CSV file"`
	FileEnclosure     string   `configKey:"file-enclosure" configUsage:"field enclosure used in the CSV file"`
	FileEscapedBy     string   `configKey:"file-escaped-by" configUsage:"escape character used in the CSV file"`
}

func DefaultFlags() *Flags {
	return &Flags{
		FileDelimiter: ",",
		FileEnclosure: `"`,
	}
}

func Command(p dependencies.Provider) *cobra.Command {
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
				id, createNew, err := utils.AskTable(cmd.Context(), d, true)
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

			file, err := upload.Run(cmd.Context(), fileUploadOpts, d)
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

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
