package file

import (
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
)

func DownloadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `download [file]`,
		Short: helpmsg.Read(`remote/file/download/short`),
		Long:  helpmsg.Read(`remote/file/download/long`),
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

			// Compose file key
			fileKey := keboola.FileKey{BranchID: branch.ID}
			if len(args) == 0 {
				allRecentFiles, err := d.KeboolaProjectAPI().ListFilesRequest(branch.ID).Send(cmd.Context())
				if err != nil {
					return err
				}
				file, err := d.Dialogs().AskFile(*allRecentFiles)
				if err != nil {
					return err
				}
				fileKey = file.FileKey
			} else if fileID, err := strconv.Atoi(args[0]); err == nil {
				fileKey.FileID = keboola.FileID(fileID)
			} else {
				return err
			}

			// Ask options
			output, err := d.Dialogs().AskFileOutput()
			if err != nil {
				return err
			}

			// Get file
			file, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(fileKey).Send(cmd.Context())
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-file-download")

			opts := download.Options{
				File:        file,
				Output:      output,
				AllowSliced: d.Options().GetBool("allow-sliced"),
			}

			return download.Run(cmd.Context(), opts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("output", "o", "", "path to the destination file or directory")
	cmd.Flags().Bool("allow-sliced", false, "output sliced files as a directory containing slices as individual files")

	return cmd
}
