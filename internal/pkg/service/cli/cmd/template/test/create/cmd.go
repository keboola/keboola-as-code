package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

type Flags struct {
	StorageAPIHost   configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken  configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	TestName         configmap.Value[string] `configKey:"test-name" configUsage:"name of the test to be created"`
	InputsFile       configmap.Value[string] `configKey:"inputs-file" configShorthand:"f" configUsage:"JSON file with inputs values"`
	Verbose          configmap.Value[bool]   `configKey:"verbose" configUsage:"show details about creating test"`
	TestProjectsFile configmap.Value[string] `configKey:"test-projects-file" configUsage:"file containing projects that could be used for templates"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create [template] [version]",
		Short: helpmsg.Read(`template/test/create/short`),
		Long:  helpmsg.Read(`template/test/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			d, err := p.LocalCommandScope(cmd.Context(), f.StorageAPIToken, dependencies.WithDefaultStorageAPIHost())
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

			tmpl, err := d.TemplateForTests(cmd.Context(), model.NewTemplateRef(repo.Definition(), templateID, versionArg), f.TestProjectsFile.Value)
			if err != nil {
				return err
			}

			// Options
			options, warnings, err := AskCreateTemplateTestOptions(cmd.Context(), d.Dialogs(), tmpl, f)
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

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
