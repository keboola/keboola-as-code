package workspace

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/detail"
)

func DetailCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `detail`,
		Short: helpmsg.Read(`remote/workspace/detail/short`),
		Long:  helpmsg.Read(`remote/workspace/detail/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Ask options
			id, err := d.Dialogs().AskWorkspaceID()
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-detail-workspace")

			return detail.Run(cmd.Context(), d, keboola.ConfigID(id))
		},
	}

	detailFlags := DetailFlags{}
	cliconfig.MustGenerateFlags(detailFlags, cmd.Flags())

	return cmd
}
