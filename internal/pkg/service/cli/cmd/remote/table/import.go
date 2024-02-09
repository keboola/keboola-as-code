package table

import (
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type ImportFlags struct {
	StorageAPIHost     string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Columns            string   `mapstructure:"columns" usage:"comma separated list of column names. If present, the first row in the CSV file is not treated as a header"`
	IncrementalLoad    bool     `mapstructure:"incremental-load" usage:"data are either added to existing data in the table or replace the existing data"`
	FileWithoutHeaders bool     `mapstructure:"file-without-headers" usage:"states if the CSV file contains headers on the first row or not"`
	PrimaryKeys        []string `mapstructure:"primary-key" usage:"primary key for the newly created table if the table doesn't exist"`
	FileDelimiter      string   `mapstructure:"file-delimiter" usage:"field delimiter used in the CSV file"`
	FileEnclosure      string   `mapstructure:"file-enclosure" usage:"field enclosure used in the CSV file"`
	FileEscapedBy      string   `mapstructure:"file-escaped-by" usage:"escape character used in the CSV file"`
}

func DefaultImportFlags() *ImportFlags {
	return &ImportFlags{
		FileDelimiter: ",",
		FileEnclosure: `"`,
	}
}

func ImportCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `import [table] [file]`,
		Short: helpmsg.Read(`remote/table/import/short`),
		Long:  helpmsg.Read(`remote/table/import/long`),
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

			var fileID int
			if len(args) < 2 {
				allRecentFiles, err := d.KeboolaProjectAPI().ListFilesRequest().Send(cmd.Context())
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
				WithoutHeaders:  d.Options().GetBool("file-without-headers"),
				PrimaryKey:      primaryKey,
				Delimiter:       d.Options().GetString("file-delimiter"),
				Enclosure:       d.Options().GetString("file-enclosure"),
				EscapedBy:       d.Options().GetString("file-escaped-by"),
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-import")

			return tableImport.Run(cmd.Context(), opts, d)
		},
	}

	cliconfig.MustGenerateFlags(DefaultImportFlags(), cmd.Flags())

	return cmd
}
