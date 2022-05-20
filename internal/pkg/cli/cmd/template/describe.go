package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	describeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/describe"
)

func DescribeCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe <template> [version]",
		Short: helpmsg.Read(`template/describe/short`),
		Long:  helpmsg.Read(`template/describe/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			if len(args) < 1 {
				return fmt.Errorf(`please enter argument with the template ID you want to use and optionally its version`)
			}

			// Get template repository
			repo, err := d.LocalTemplateRepository()
			if err != nil {
				return err
			}

			// Optional version argument
			var versionArg string
			if len(args) > 1 {
				versionArg = args[1]
			}

			// Load template
			template, err := d.Template(model.NewTemplateRef(repo.Ref(), args[0], versionArg))
			if err != nil {
				return err
			}

			// Describe template
			return describeOp.Run(template, d)
		},
	}

	return cmd
}
