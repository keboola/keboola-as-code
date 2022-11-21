package remote

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
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
		workspace.Commands(p),
	)

	return cmd
}
