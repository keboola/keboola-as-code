package download

import (
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
)

type Flags struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Output         string `configKey:"output" configShorthand:"o" configUsage:"path to the destination file or directory"`
	AllowSliced    bool   `configKey:"allow-sliced" configUsage:"output sliced files as a directory containing slices as individual files"`
}

func Command(p dependencies.Provider) *cobra.Command {
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

			// Ask options
			output, err := d.Dialogs().AskFileOutput()
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-file-download")

			var fileID int
			if len(args) == 0 {
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
				fileID, err = strconv.Atoi(args[0])
				if err != nil {
					return err
				}
			}

			file, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(fileID).Send(cmd.Context())
			if err != nil {
				return err
			}

			opts := download.Options{
				File:        file,
				Output:      output,
				AllowSliced: d.Options().GetBool("allow-sliced"),
			}

			return download.Run(cmd.Context(), opts, d)
		},
	}

	cliconfig.MustGenerateFlags(Flags{}, cmd.Flags())

	return cmd
}
