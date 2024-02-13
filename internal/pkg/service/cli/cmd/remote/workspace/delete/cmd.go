package delete

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/delete"
)

type Flags struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	WorkspaceID    string `configKey:"workspace-id" configShorthand:"W" configUsage:"id of the workspace to delete"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete`,
		Short: helpmsg.Read(`remote/workspace/delete/short`),
		Long:  helpmsg.Read(`remote/workspace/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Options
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(cmd.Context())
			if err != nil {
				return errors.Errorf("cannot find default branch: %w", err)
			}

			allWorkspaces, err := d.KeboolaProjectAPI().ListWorkspaces(cmd.Context(), branch.ID)
			if err != nil {
				return err
			}

			sandbox, err := d.Dialogs().AskWorkspace(allWorkspaces)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-list-workspace")

			return deleteOp.Run(cmd.Context(), d, branch.ID, sandbox)
		},
	}

	deleteFlags := Flags{}
	cliconfig.MustGenerateFlags(deleteFlags, cmd.Flags())

	return cmd
}
