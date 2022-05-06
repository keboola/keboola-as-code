package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	describeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/describe"
)

func DescribeCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe <template> <version>",
		Short: helpmsg.Read(`template/describe/short`),
		Long:  helpmsg.Read(`template/describe/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// Get template repository
			repo, err := d.LocalTemplateRepository()
			if err != nil {
				return err
			}

			// Template definition
			templateDef, err := model.NewTemplateRefFromString(repo.Ref(), args[0], args[1])
			if err != nil {
				return err
			}

			// Load template
			template, err := d.Template(templateDef)
			if err != nil {
				return err
			}

			// Describe template
			return describeOp.Run(template, d)
		},
	}

	return cmd
}
