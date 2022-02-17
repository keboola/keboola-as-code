package ci

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `ci`,
		Short: helpmsg.Read(`ci/short`),
		Long:  helpmsg.Read(`ci/long`),
	}
	cmd.AddCommand(
		WorkflowsCommand(d),
	)
	return cmd
}
