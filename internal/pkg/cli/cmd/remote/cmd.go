package remote

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/remote/workspace"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
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
