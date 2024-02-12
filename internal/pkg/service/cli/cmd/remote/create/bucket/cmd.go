package bucket

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/bucket"
)

type Flags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"if command is run outside the project directory"`
	Description    string `mapstructure:"description" usage:"bucket description"`
	DisplayName    string `mapstructure:"display-name" usage:"display name for the UI"`
	Name           string `mapstructure:"name" usage:"name of the bucket"`
	Stage          string `mapstructure:"stage" usage:"stage, allowed values: in, out"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bucket",
		Short: helpmsg.Read(`remote/create/bucket/short`),
		Long:  helpmsg.Read(`remote/create/bucket/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Options
			opts, err := d.Dialogs().AskCreateBucket()
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-bucket")

			return bucket.Run(cmd.Context(), opts, d)
		},
	}

	cliconfig.MustGenerateFlags(Flags{}, cmd.Flags())

	return cmd
}
