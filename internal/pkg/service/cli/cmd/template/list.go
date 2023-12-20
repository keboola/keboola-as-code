package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/list"
)

func ListCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: helpmsg.Read(`template/list/short`),
		Long:  helpmsg.Read(`template/list/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in template repository
			repo, d, err := p.LocalRepository(cmd.Context(), dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			// Describe template
			return listOp.Run(cmd.Context(), repo, d)
		},
	}

	return cmd
}
