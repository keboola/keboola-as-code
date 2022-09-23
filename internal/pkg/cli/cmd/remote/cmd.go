package remote

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/remote/workspace"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

func Commands(p dependencies.Provider, envs *env.Map) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `remote`,
		Short: helpmsg.Read(`remote/short`),
		Long:  helpmsg.Read(`remote/long`),
	}
	cmd.AddCommand(
		CreateCommand(p),
	)

	// Workspace commands are not finished yet.
	if envs.Get(`KBC_DBT_PRIVATE_BETA`) == `true` {
		cmd.AddCommand(workspace.Commands(p))
	}

	return cmd
}
