package download

import (
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
)

type Flags struct {
	StorageAPIHost    configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken   configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Output            configmap.Value[string] `configKey:"output" configShorthand:"o" configUsage:"path to the destination file or directory"`
	AllowSliced       configmap.Value[bool]   `configKey:"allow-sliced" configUsage:"output sliced files as a directory containing slices as individual files"`
	WithoutDecompress configmap.Value[bool]   `configKey:"without-decompress" configUsage:"do not decompress the downloaded files or sliced files."`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `download [file]`,
		Short: helpmsg.Read(`remote/file/download/short`),
		Long:  helpmsg.Read(`remote/file/download/long`),
		Args:  cobra.MaximumNArgs(1),
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
			output, err := d.Dialogs().AskFileOutput(f.Output)
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
				File:              file,
				Output:            output,
				AllowSliced:       f.AllowSliced.Value,
				WithOutDecompress: f.WithoutDecompress,
			}

			return download.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
