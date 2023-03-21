package table

import (
	"fmt"
	"path"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	common "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/upload"
)

func UploadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [file] [table]`,
		Short: helpmsg.Read(`remote/file/upload/short`),
		Long:  helpmsg.Read(`remote/file/upload/long`),
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

			var filePath string
			if len(args) < 1 {
				filePath = d.Dialogs().AskFileInput()
			} else {
				filePath = args[0]
			}
			if !d.Fs().Exists(filePath) {
				return errors.Errorf(`file "%s" not found`, filePath)
			}

			var tableID keboola.TableID
			if len(args) < 2 {
				if d.Options().IsSet("incremental-load") {
					allTables, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithColumns()).Send(d.CommandCtx())
					if err != nil {
						return err
					}
					table, err := d.Dialogs().AskTable(d.Options(), *allTables)
					if err != nil {
						return err
					}
					tableID = table.ID
				} else {
					tableID, err = keboola.ParseTableID(d.Dialogs().AskTableID())
					if err != nil {
						return err
					}
				}
			} else {
				tableID, err = keboola.ParseTableID(args[1])
				if err != nil {
					return err
				}
			}

			opts, err := parseUploadOptions(d.Options(), filePath, tableID)
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-table-upload")

			return upload.Run(d.CommandCtx(), opts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().Bool("incremental-load", false, "data are either added to existing data in the table or replace the existing data")

	return cmd
}

func parseUploadOptions(options *options.Options, filePath string, tableID keboola.TableID) (upload.Options, error) {
	o := upload.Options{}

	o.TableID = tableID
	o.FilePath = filePath

	if path.Base(o.FilePath) == "." || path.Base(o.FilePath) == "/" {
		return upload.Options{}, errors.Errorf(`invalid file path "%s"`, o.FilePath)
	}
	o.FileName = fmt.Sprintf("%s_%s", idgenerator.Random(10), path.Base(o.FilePath))
	o.IncrementalLoad = options.GetBool("incremental-load")

	return o, nil
}
