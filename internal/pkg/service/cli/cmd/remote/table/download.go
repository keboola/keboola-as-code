package table

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

func DownloadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `download [table]`,
		Short: helpmsg.Read(`remote/table/download/short`),
		Long:  helpmsg.Read(`remote/table/download/long`),
		Args:  cobra.MaximumNArgs(1),
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
			if len(args) == 0 {
				tableKey, _, err = askTable(cmd.Context(), d, branch.ID, false)
				if err != nil {
					return err
				}
			} else if id, err := keboola.ParseTableID(args[0]); err == nil {
				tableKey.TableID = id
			} else {
				return err
			}

			fileOutput, err := d.Dialogs().AskFileOutput()
			if err != nil {
				return err
			}

			unloadOpts, err := parseUnloadOptions(d.Options(), tableKey)
			if err != nil {
				return err
			}

			unloadedFile, err := unload.Run(cmd.Context(), unloadOpts, d)
			if err != nil {
				return err
			}

			fileWithCredentials, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(unloadedFile.FileKey).Send(cmd.Context())
			if err != nil {
				return err
			}

			downloadOpts := download.Options{
				File:        fileWithCredentials,
				Output:      fileOutput,
				AllowSliced: d.Options().GetBool("allow-sliced"),
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-unload")

			return download.Run(cmd.Context(), downloadOpts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("changed-since", "", "only export rows imported after this date")
	cmd.Flags().String("changed-until", "", "only export rows imported before this date")
	cmd.Flags().StringSlice("columns", []string{}, "comma-separated list of columns to export")
	cmd.Flags().Uint("limit", 0, "limit the number of exported rows")
	cmd.Flags().String("where", "", "filter columns by value")
	cmd.Flags().String("order", "", "order by one or more columns")
	cmd.Flags().String("format", "csv", "output format (json/csv)")
	cmd.Flags().String("timeout", "2m", "how long to wait for the unload job to finish")
	cmd.Flags().StringP("output", "o", "", "path to the destination file or directory")
	cmd.Flags().Bool("allow-sliced", false, "output sliced files as a directory containing slices as individual files")

	return cmd
}
