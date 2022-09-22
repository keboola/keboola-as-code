package dbt

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/dbt/generate"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `dbt`,
		Short: helpmsg.Read(`dbt/short`),
		Long:  helpmsg.Read(`dbt/long`),
	}
	cmd.AddCommand(
		InitCommand(p),
		generate.Commands(p),
	)
	return cmd
}
