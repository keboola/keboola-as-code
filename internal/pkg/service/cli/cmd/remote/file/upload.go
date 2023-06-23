package file

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
)

func UploadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [file]`,
		Short: helpmsg.Read(`remote/file/upload/short`),
		Long:  helpmsg.Read(`remote/file/upload/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Ask options
			opts, err := d.Dialogs().AskUploadFile("", "")
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-file-upload")

			_, err = upload.Run(d.CommandCtx(), opts, d)
			return err
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("data", "", "path to the file to be uploaded")
	cmd.Flags().String("file-name", "", "name of the file to be created")
	cmd.Flags().String("file-tags", "", "comma-separated list of tags")

	return cmd
}
