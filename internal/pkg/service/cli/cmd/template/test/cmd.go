package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/test/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/test/run"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `test`,
		Short: helpmsg.Read(`template/test/short`),
		Long:  helpmsg.Read(`template/test/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return run.Command(d).RunE(cmd, args)
		},
	}

	runTestFlags := run.Flags{}
	cliconfig.MustGenerateFlags(runTestFlags, cmd.Flags())

	cmd.AddCommand(
		create.Command(d),
		run.Command(d),
	)
	return cmd
}
