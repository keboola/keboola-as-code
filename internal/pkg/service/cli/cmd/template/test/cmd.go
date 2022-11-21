package test

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `test`,
		Short: helpmsg.Read(`template/test/short`),
		Long:  helpmsg.Read(`template/test/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunCommand(d).RunE(cmd, args)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().String("test-name", "", "name of a single test to be run")
	cmd.Flags().Bool("local-only", false, "run a local test only")
	cmd.Flags().Bool("remote-only", false, "run a remote test only")
	cmd.Flags().Bool("verbose", false, "show details about running tests")

	cmd.AddCommand(
		CreateCommand(d),
		RunCommand(d),
	)
	return cmd
}
