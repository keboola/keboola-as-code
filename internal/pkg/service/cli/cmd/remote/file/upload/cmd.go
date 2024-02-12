package upload

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
)

type Flags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Data           string `mapstructure:"data" usage:"path to the file to be uploaded"`
	FileName       string `mapstructure:"file-name" usage:"name of the file to be created"`
	FileTags       string `mapstructure:"file-tags" usage:"comma-separated list of tags"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [file]`,
		Short: helpmsg.Read(`remote/file/upload/short`),
		Long:  helpmsg.Read(`remote/file/upload/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Ask options
			opts, err := d.Dialogs().AskUploadFile("", "")
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-file-upload")

			_, err = upload.Run(cmd.Context(), opts, d)
			return err
		},
	}

	cliconfig.MustGenerateFlags(Flags{}, cmd.Flags())

	return cmd
}
