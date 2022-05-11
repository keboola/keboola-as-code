package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/list"
)

func ListCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: helpmsg.Read(`template/list/short`),
		Long:  helpmsg.Read(`template/list/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// Get template repository
			repo, err := d.LocalTemplateRepository()
			if err != nil {
				return err
			}

			// Describe template
			return listOp.Run(repo, d)
		},
	}

	return cmd
}
