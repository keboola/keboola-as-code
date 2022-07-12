package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	testOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test"
)

func TestCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test <template> [version]",
		Short: helpmsg.Read(`template/test/short`),
		Long:  helpmsg.Read(`template/test/long`),
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

			// Test template
			return testOp.Run(template, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().String("test-name", "", "name of a single test to be run")
	cmd.Flags().Bool("local-test", false, "run a local test only")

	return cmd
}
