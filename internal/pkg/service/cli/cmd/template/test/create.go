package test

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

func CreateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create [template] [version]",
		Short: helpmsg.Read(`template/test/create/short`),
		Long:  helpmsg.Read(`template/test/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := p.LocalCommandScope(cmd.Context(), dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			if len(args) < 1 {
				return errors.New(`please enter argument with the template ID you want to use and optionally its version`)
			}
			templateID := args[0]

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

			tmpl, err := d.Template(d.CommandCtx(), model.NewTemplateRef(repo.Definition(), templateID, versionArg))
			if err != nil {
				return err
			}

			// Options
			options, warnings, err := d.Dialogs().AskCreateTemplateTestOptions(cmd.Context(), tmpl)
			if err != nil {
				return err
			}

			err = createOp.Run(d.CommandCtx(), tmpl, options, d)
			if err != nil {
				return err
			}

			if len(warnings) > 0 {
				for _, w := range warnings {
					d.Logger().WarnfCtx(cmd.Context(), w)
				}
			}
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().String("test-name", "", "name of the test to be created")
	cmd.Flags().StringP(`inputs-file`, "f", ``, "JSON file with inputs values")
	cmd.Flags().Bool("verbose", false, "show details about creating test")

	return cmd
}
