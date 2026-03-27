package deleteworkspace

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	wsinfo "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/delete"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/list"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	WorkspaceID     configmap.Value[string] `configKey:"workspace-id" configShorthand:"W" configUsage:"id of the workspace to delete"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete`,
		Short: helpmsg.Read(`remote/workspace/delete/short`),
		Long:  helpmsg.Read(`remote/workspace/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Get default branch
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(cmd.Context())
			if err != nil {
				return errors.Errorf("cannot get default branch: %w", err)
			}

			// Fetch Python/R workspaces and editor sessions in parallel.
			var pyRWorkspaces []*wsinfo.WorkspaceWithConfig
			var allConfigs []*keboola.Config
			var sessions []*keboola.EditorSession

			grp, grpCtx := errgroup.WithContext(cmd.Context())
			grp.Go(func() error {
				var e error
				pyRWorkspaces, allConfigs, e = listOp.ListPyRWorkspaces(grpCtx, d.KeboolaProjectAPI(), branch.ID)
				return e
			})
			grp.Go(func() error {
				result, e := d.KeboolaProjectAPI().ListEditorSessionsRequest().Send(grpCtx)
				if e != nil {
					return e
				}
				sessions = *result
				return nil
			})
			if err := grp.Wait(); err != nil {
				return err
			}

			// Build config name map for editor session name lookup.
			configNameMap := make(map[string]string)
			for _, c := range allConfigs {
				configNameMap[c.ID.String()] = c.Name
			}

			// Build combined list: Python/R workspaces + SQL editor sessions.
			allWorkspaces := make([]*wsinfo.WorkspaceWithConfig, 0, len(pyRWorkspaces)+len(sessions))
			allWorkspaces = append(allWorkspaces, pyRWorkspaces...)
			for _, s := range sessions {
				name := configNameMap[s.ConfigurationID]
				allWorkspaces = append(allWorkspaces, &wsinfo.WorkspaceWithConfig{
					Config: &keboola.Config{
						ConfigKey: keboola.ConfigKey{ID: keboola.ConfigID(s.ConfigurationID)},
						Name:      name,
					},
					Session: s,
				})
			}


			ws, err := d.Dialogs().AskWorkspace(allWorkspaces, f.WorkspaceID)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-workspace-delete")

			return deleteOp.Run(cmd.Context(), d, branch.ID, ws)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
