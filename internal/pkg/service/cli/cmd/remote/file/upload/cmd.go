package upload

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"master-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Data            configmap.Value[string] `configKey:"data" configUsage:"path to the file to be uploaded"`
	FileName        configmap.Value[string] `configKey:"file-name" configUsage:"name of the file to be created"`
	FileTags        configmap.Value[string] `configKey:"file-tags" configUsage:"comma-separated list of tags"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upload [file]`,
		Short: helpmsg.Read(`remote/file/upload/short`),
		Long:  helpmsg.Read(`remote/file/upload/long`),
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
			opts, err := d.Dialogs().AskUploadFile(branch.BranchKey, dialog.AskUpload{
				Input:       "",
				DefaultName: "",
				FileName:    f.FileName,
				FileTag:     f.FileTags,
				Data:        f.Data,
			})
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-file-upload")

			_, err = upload.Run(cmd.Context(), opts, d)
			return err
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
