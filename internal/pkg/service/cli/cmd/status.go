package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/status"
)

func StatusCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: helpmsg.Read(`status/short`),
		Long:  helpmsg.Read(`status/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}
			return status.Run(d.CommandCtx(), d)
		},
	}

	return cmd
}
