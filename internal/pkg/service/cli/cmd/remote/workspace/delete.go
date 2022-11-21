package workspace

import (
	"time"

	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
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
			start := time.Now()

			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			defer func() { d.EventSender().SendCmdEvent(d.CommandCtx(), start, cmdErr, "remote-list-workspace") }()

			branch, err := storageapi.GetDefaultBranchRequest().Send(d.CommandCtx(), d.StorageApiClient())
			if err != nil {
				return errors.Errorf("cannot find default branch: %w", err)
			}

			allWorkspaces, err := sandboxesapi.List(d.CommandCtx(), d.StorageApiClient(), d.SandboxesApiClient(), branch.ID)
			if err != nil {
				return err
			}

			sandbox, err := d.Dialogs().AskWorkspace(d.Options(), allWorkspaces)
			if err != nil {
				return err
			}

			err = deleteOp.Run(d.CommandCtx(), d, branch.ID, sandbox)
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("workspace-id", "W", "", "id of the workspace to delete")

	return cmd
}
