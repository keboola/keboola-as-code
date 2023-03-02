package file

import (
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	common "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
)

func DownloadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `download [file]`,
		Short: helpmsg.Read(`remote/file/download/short`),
		Long:  helpmsg.Read(`remote/file/download/long`),
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			output, err := baseDeps.Dialogs().AskFileOutput(baseDeps.Options())
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand(common.WithoutMasterToken())
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-file-download")

			var fileID int
			if len(args) == 0 {
				allRecentFiles, err := d.KeboolaProjectAPI().ListFilesRequest().Send(d.CommandCtx())
				if err != nil {
					return err
				}
				file, err := d.Dialogs().AskFile(*allRecentFiles)
				if err != nil {
					return err
				}
				fileID = file.ID
			} else {
				fileID, err = strconv.Atoi(args[0])
				if err != nil {
					return err
				}
			}

			file, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(fileID).Send(d.CommandCtx())
			if err != nil {
				return err
			}

			return download.Run(d.CommandCtx(), download.Options{File: file, Output: output}, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("output", "o", "", "path to the destination file (if the file is not sliced) or directory (if the file is sliced)")

	return cmd
}
