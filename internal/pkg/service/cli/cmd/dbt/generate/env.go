package generate

import (
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
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

			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			branch, err := storageapi.GetDefaultBranchRequest().Send(d.CommandCtx(), d.StorageApiClient())
			if err != nil {
				return errors.Errorf("cannot find default branch: %w", err)
			}

			// Get all Snowflake workspaces for the dialog
			allWorkspaces, err := sandboxesapi.List(d.CommandCtx(), d.StorageApiClient(), d.SandboxesApiClient(), branch.ID)
			if err != nil {
				return err
			}
			snowflakeWorkspaces := make([]*sandboxesapi.SandboxWithConfig, 0)
			for _, w := range allWorkspaces {
				if w.Sandbox.Type == sandboxesapi.TypeSnowflake {
					snowflakeWorkspaces = append(snowflakeWorkspaces, w)
				}
			}

			// Options
			opts, err := d.Dialogs().AskGenerateEnv(d, snowflakeWorkspaces)
			if err != nil {
				return err
			}

			return env.Run(d.CommandCtx(), opts, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("target-name", "T", "", "target name of the profile")
	cmd.Flags().StringP("workspace-id", "W", "", "id of the workspace to use")

	return cmd
}
