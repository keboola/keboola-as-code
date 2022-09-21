package generate

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `generate`,
		Short: helpmsg.Read(`dbt/generate/short`),
		Long:  helpmsg.Read(`dbt/generate/long`),
	}
	cmd.AddCommand(
		EnvCommand(p),
		ProfilesCommand(p),
	)
	return cmd
}
