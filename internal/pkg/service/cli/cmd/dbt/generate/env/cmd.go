package env

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cobra"

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
		RunE: func(cmd *cobra.Command, args []string) error {
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

			// Get all Snowflake workspaces for the dialog
			allWorkspaces, err := d.KeboolaProjectAPI().ListWorkspaces(cmd.Context(), branch.ID)
			if err != nil {
				return err
			}
			snowflakeWorkspaces := make([]*keboola.WorkspaceWithConfig, 0)
			for _, w := range allWorkspaces {
				if w.Workspace.Type == keboola.WorkspaceTypeSnowflake {
					snowflakeWorkspaces = append(snowflakeWorkspaces, w)
				}
			}

			opts, err := AskGenerateEnv(branch.BranchKey, d.Dialogs(), snowflakeWorkspaces, f)
			if err != nil {
				return err
			}

			return env.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
