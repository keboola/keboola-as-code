package test

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

func CreateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create [template] [version]",
		Short: helpmsg.Read(`template/test/create/short`),
		Long:  helpmsg.Read(`template/test/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			if len(args) < 1 {
				return fmt.Errorf(`please enter argument with the template ID you want to use and optionally its version`)
			}
			templateId := args[0]

			// Get template repository
			repo, _, err := d.LocalTemplateRepository(d.CommandCtx())
			if err != nil {
				return err
			}

			// Optional version argument
			var versionArg string
			if len(args) > 1 {
				versionArg = args[1]
			}

			tmpl, err := d.Template(d.CommandCtx(), model.NewTemplateRef(repo.Ref(), templateId, versionArg))
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateTemplateTestOptions(tmpl.Inputs(), d.Options())
			if err != nil {
				return err
			}

			return createOp.Run(d.CommandCtx(), tmpl, options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().String("test-name", "", "name of the test to be created")
	cmd.Flags().StringP(`inputs-file`, "f", ``, "JSON file with inputs values")
	cmd.Flags().Bool("verbose", false, "show details about creating test")

	return cmd
}
