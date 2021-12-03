package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func DescribeCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe",
		Short: helpmsg.Read(`template/describe/short`),
		Long:  helpmsg.Read(`template/describe/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf(`not implemented`)
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	return cmd
}
