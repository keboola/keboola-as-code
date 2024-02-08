package table

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
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

			// Ask options
			var tableID keboola.TableID
			if len(args) == 0 {
				tableID, _, err = askTable(cmd.Context(), d, false)
				if err != nil {
					return err
				}
			} else {
				id, err := keboola.ParseTableID(args[0])
				if err != nil {
					return err
				}
				tableID = id
			}

			fileOutput, err := d.Dialogs().AskFileOutput()
			if err != nil {
				return err
			}

			unloadOpts, err := parseUnloadOptions(d.Options(), tableID)
			if err != nil {
				return err
			}

			unloadedFile, err := unload.Run(cmd.Context(), unloadOpts, d)
			if err != nil {
				return err
			}

			fileWithCredentials, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(unloadedFile.ID).Send(cmd.Context())
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

	downloadFlags := DownloadFlags{
		Limit:   0,
		Columns: []string{},
		Format:  "csv",
		Timeout: "2m",
	}
	_ = cliconfig.GenerateFlags(downloadFlags, cmd.Flags())
	return cmd
}
