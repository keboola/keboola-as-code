package workspace

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/delete"
)

func DeleteCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete`,
		Short: helpmsg.Read(`remote/workspace/delete/short`),
		Long:  helpmsg.Read(`remote/workspace/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope()
			if err != nil {
				return err
			}

			// Options
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(d.CommandCtx())
			if err != nil {
				return errors.Errorf("cannot find default branch: %w", err)
			}

			allWorkspaces, err := d.KeboolaProjectAPI().ListWorkspaces(d.CommandCtx(), branch.ID)
			if err != nil {
				return err
			}

			sandbox, err := d.Dialogs().AskWorkspace(allWorkspaces)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-list-workspace")

			return deleteOp.Run(d.CommandCtx(), d, branch.ID, sandbox)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("workspace-id", "W", "", "id of the workspace to delete")

	return cmd
}
