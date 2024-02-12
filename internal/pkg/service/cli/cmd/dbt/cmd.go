package dbt

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/generate"
	dbt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/init"
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
		dbt.Command(p),
		generate.Commands(p),
	)
	return cmd
}
