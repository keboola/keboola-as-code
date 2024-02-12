package dbt

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/init"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/generate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `dbt`,
		Short: helpmsg.Read(`dbt/short`),
		Long:  helpmsg.Read(`dbt/long`),
	}
	cmd.AddCommand(
		init.Command(p),
		generate.Commands(p),
	)
	return cmd
}
