package env

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	TargetName      configmap.Value[string] `configKey:"target-name" configShorthand:"T" configUsage:"target name of the profile"`
	WorkspaceID     configmap.Value[string] `configKey:"workspace-id" configShorthand:"W" configUsage:"id of the workspace to use"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `env`,
		Short: helpmsg.Read(`dbt/generate/env/short`),
		Long:  helpmsg.Read(`dbt/generate/env/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Check that we are in dbt directory
			if _, _, err := p.LocalDbtProject(cmd.Context()); err != nil {
				return err
			}

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

			// Fetch Python/R sandbox workspaces, SQL editor sessions, and sandbox configs in parallel.
			var pyRWorkspaces []*keboola.SandboxWorkspaceWithConfig
			var sessions []*keboola.EditorSession
			var sandboxConfigs []*keboola.Config

			grp, grpCtx := errgroup.WithContext(cmd.Context())
			grp.Go(func() error {
				var e error
				pyRWorkspaces, e = d.KeboolaProjectAPI().ListSandboxWorkspaces(grpCtx, branch.ID)
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
			grp.Go(func() error {
				result, e := d.KeboolaProjectAPI().ListSandboxWorkspaceConfigRequest(branch.ID).Send(grpCtx)
				if e != nil {
					return e
				}
				sandboxConfigs = *result
				return nil
			})
			if err := grp.Wait(); err != nil {
				return err
			}

			// Build config name map for editor session name lookup.
			configNameMap := make(map[string]string)
			for _, c := range sandboxConfigs {
				configNameMap[c.ID.String()] = c.Name
			}

			// Build combined workspace list: Python/R + SQL editor sessions.
			// For SQL sessions, SandboxWorkspace.ID stores the EditorSessionID for credential lookup.
			allWorkspaces := make([]*keboola.SandboxWorkspaceWithConfig, 0, len(pyRWorkspaces)+len(sessions))
			allWorkspaces = append(allWorkspaces, pyRWorkspaces...)
			for _, s := range sessions {
				name := configNameMap[s.ConfigurationID]
				allWorkspaces = append(allWorkspaces, &keboola.SandboxWorkspaceWithConfig{
					Config: &keboola.Config{
						ConfigKey: keboola.ConfigKey{ID: keboola.ConfigID(s.ConfigurationID)},
						Name:      name,
					},
					SandboxWorkspace: &keboola.SandboxWorkspace{
						ID:   keboola.SandboxWorkspaceID(s.ID),
						Type: keboola.SandboxWorkspaceType(s.BackendType),
					},
				})
			}

			opts, err := AskGenerateEnv(cmd.Context(), branch.BranchKey, branch.ID, d.Dialogs(), allWorkspaces, sessions, f, p.BaseScope().Environment(), d.KeboolaProjectAPI())
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "dbt-generate-env")

			return env.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
