package sync

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `sync`,
		Short: helpmsg.Read(`sync/short`),
		Long:  helpmsg.Read(`sync/long`),
	}
	cmd.AddCommand(
		InitCommand(d),
		PullCommand(d),
		PushCommand(d),
		DiffCommand(d),
	)
	return cmd
}
