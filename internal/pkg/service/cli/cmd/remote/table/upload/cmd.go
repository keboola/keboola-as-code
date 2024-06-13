package upload

import (
	"fmt"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	utils2 "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	fileUpload "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type Flags struct {
	StorageAPIHost    configmap.Value[string]   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken   configmap.Value[string]   `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Columns           configmap.Value[string]   `configKey:"columns" configUsage:"comma separated list of column names. If present, the first row in the CSV file is not treated as a header"`
	IncrementalLoad   configmap.Value[bool]     `configKey:"incremental-load" configUsage:"data are either added to existing data in the table or replace the existing data"`
	FileWithoutHeader configmap.Value[bool]     `configKey:"file-without-headers" configUsage:"states if the CSV file contains headers on the first row or not"`
	PrimaryKeys       configmap.Value[[]string] `configKey:"primary-key" configUsage:"primary key for the newly created table if the table doesn't exist"`
	FileName          configmap.Value[string]   `configKey:"file-name" configUsage:"name of the file to be created"`
	FileTags          configmap.Value[string]   `configKey:"file-tags" configUsage:"comma-separated list of file tags"`
	FileDelimiter     configmap.Value[string]   `configKey:"file-delimiter" configUsage:"field delimiter used in the CSV file"`
	FileEnclosure     configmap.Value[string]   `configKey:"file-enclosure" configUsage:"field enclosure used in the CSV file"`
	FileEscapedBy     configmap.Value[string]   `configKey:"file-escaped-by" configUsage:"escape character used in the CSV file"`
}

func DefaultFlags() Flags {
	return Flags{
		FileDelimiter: configmap.NewValue(","),
		FileEnclosure: configmap.NewValue(`"`),
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [table] [file]`,
		Short: helpmsg.Read(`remote/table/upload/short`),
		Long:  helpmsg.Read(`remote/table/upload/long`),
		Args:  cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken, dependencies.WithoutMasterToken())
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
				key, createNew, err := utils2.AskTable(cmd.Context(), d, branch.ID, true, configmap.NewValue(tableKey.TableID.String()))
				if err != nil {
					return err
				}
				tableKey = key
				if createNew {
					primaryKey = d.Dialogs().AskPrimaryKey(f.PrimaryKeys)
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
			fileUploadOpts, err := d.Dialogs().AskUploadFile(branch.BranchKey, dialog.AskUpload{
				Input:       inputFile,
				DefaultName: fileName,
				FileName:    f.FileName,
				FileTag:     f.FileTags,
			})
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
				Delimiter:       f.FileDelimiter.Value,
				Enclosure:       f.FileEnclosure.Value,
				EscapedBy:       f.FileEscapedBy.Value,
				IncrementalLoad: f.IncrementalLoad.Value,
				WithoutHeaders:  f.FileWithoutHeader.Value,
				PrimaryKey:      primaryKey,
			}

			if f.Columns.Value != "" {
				tableImportOpts.Columns = strings.Split(f.Columns.Value, ",")
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-upload")

			return tableImport.Run(cmd.Context(), tableImportOpts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
