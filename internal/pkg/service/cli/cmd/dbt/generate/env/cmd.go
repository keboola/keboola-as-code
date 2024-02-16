package env

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	"github.com/spf13/cobra"

	e "github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     configmap.Value[string] `configKey:"target-name" configShorthand:"T" configUsage:"target name of the profile"`
	WorkspaceID    configmap.Value[string] `configKey:"workspace-id" configShorthand:"W" configUsage:"id of the workspace to use"`
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

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			flags := Flags{}
			err = configmap.Bind(configmap.BindConfig{
				Flags:     cmd.Flags(),
				Args:      args,
				EnvNaming: e.NewNamingConvention("KBC_"),
				Envs:      e.Empty(),
			}, &flags)
			if err != nil {
				return err
			}

			// Options
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(cmd.Context())
			if err != nil {
				return errors.Errorf("cannot find default branch: %w", err)
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
			opts, err := AskGenerateEnv(snowflakeWorkspaces, d.Dialogs(), flags)
			if err != nil {
				return err
			}

			return env.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
