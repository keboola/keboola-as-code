package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/status"
)

func StatusCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: helpmsg.Read(`status/short`),
		Long:  helpmsg.Read(`status/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d, err := p.LocalCommandScope(cmd.Context(), dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}
			return status.Run(cmd.Context(), d)
		},
	}

	return cmd
}
