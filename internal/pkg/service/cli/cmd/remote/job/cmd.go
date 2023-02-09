package job

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `job`,
		Short: helpmsg.Read(`remote/job/short`),
		Long:  helpmsg.Read(`remote/job/long`),
	}
	cmd.AddCommand(
		RunCommand(p),
	)

	return cmd
}
