package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

type Flags struct {
	TestName   string `mapstructure:"test-name" usage:"name of the test to be created"`
	InputsFile string `mapstructure:"inputs-file" shorthand:"f" usage:"JSON file with inputs values"`
	Verbose    bool   `mapstructure:"verbose" usage:"show details about creating test"`
}

func Command(p dependencies.Provider) *cobra.Command {
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
			repo, _, err := d.LocalTemplateRepository(cmd.Context())
			if err != nil {
				return err
			}

			// Optional version argument
			var versionArg string
			if len(args) > 1 {
				versionArg = args[1]
			}

			tmpl, err := d.Template(cmd.Context(), model.NewTemplateRef(repo.Definition(), templateID, versionArg))
			if err != nil {
				return err
			}

			// Options
			options, warnings, err := d.Dialogs().AskCreateTemplateTestOptions(cmd.Context(), tmpl)
			if err != nil {
				return err
			}

			err = createOp.Run(cmd.Context(), tmpl, options, d)
			if err != nil {
				return err
			}

			if len(warnings) > 0 {
				for _, w := range warnings {
					d.Logger().Warnf(cmd.Context(), w)
				}
			}
			return nil
		},
	}

	cliconfig.MustGenerateFlags(Flags{}, cmd.Flags())

	return cmd
}
