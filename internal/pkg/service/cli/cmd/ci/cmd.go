package ci

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci/workflow"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `ci`,
		Short: helpmsg.Read(`ci/short`),
		Long:  helpmsg.Read(`ci/long`),
	}
	cmd.AddCommand(
		workflow.WorkflowsCommand(p),
	)
	return cmd
}
