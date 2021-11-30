package sync

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:  `sync`,
		Long: helpmsg.Read(`sync/long`),
	}
	cmd.AddCommand(
		InitCommand(d),
		DiffCommand(d),
		PullCommand(d),
		PushCommand(d),
	)
	return cmd
}
