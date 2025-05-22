package test

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/test/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/test/run"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `test`,
		Short: helpmsg.Read(`template/test/short`),
		Long:  helpmsg.Read(`template/test/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			return run.Command(d).RunE(cmd, args)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), run.Flags{})

	cmd.AddCommand(
		create.Command(d),
		run.Command(d),
	)
	return cmd
}
