package generate

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

func EnvCommand(p dependencies.Provider) *cobra.Command {
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
			opts, err := d.Dialogs().AskGenerateEnv(snowflakeWorkspaces)
			if err != nil {
				return err
			}

			return env.Run(cmd.Context(), opts, d)
		},
	}

	envFlags := EnvFlags{}
	_ = cliconfig.GenerateFlags(envFlags, cmd.Flags())

	return cmd
}
