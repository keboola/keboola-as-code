package _import

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type Flags struct {
	StorageAPIHost     string   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Columns            string   `configKey:"columns" configUsage:"comma separated list of column names. If present, the first row in the CSV file is not treated as a header"`
	IncrementalLoad    bool     `configKey:"incremental-load" configUsage:"data are either added to existing data in the table or replace the existing data"`
	FileWithoutHeaders bool     `configKey:"file-without-headers" configUsage:"states if the CSV file contains headers on the first row or not"`
	PrimaryKeys        []string `configKey:"primary-key" configUsage:"primary key for the newly created table if the table doesn't exist"`
	FileDelimiter      string   `configKey:"file-delimiter" configUsage:"field delimiter used in the CSV file"`
	FileEnclosure      string   `configKey:"file-enclosure" configUsage:"field enclosure used in the CSV file"`
	FileEscapedBy      string   `configKey:"file-escaped-by" configUsage:"escape character used in the CSV file"`
}

func DefaultFlags() *Flags {
	return &Flags{
		FileDelimiter: ",",
		FileEnclosure: `"`,
	}
}

func Command(p dependencies.Provider) *cobra.Command {
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

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
