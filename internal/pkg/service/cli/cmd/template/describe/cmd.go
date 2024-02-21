package describe

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	describeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/describe"
)

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe <template> [version]",
		Short: helpmsg.Read(`template/describe/short`),
		Long:  helpmsg.Read(`template/describe/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New(`please enter argument with the template ID you want to use and optionally its version`)
			}

			// Command must be used in template repository
			repo, d, err := p.LocalRepository(cmd.Context(), dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			// Optional version argument
			var versionArg string
			if len(args) > 1 {
				versionArg = args[1]
			}

			// Load template
			template, err := d.Template(cmd.Context(), model.NewTemplateRef(repo.Definition(), args[0], versionArg))
			if err != nil {
				return err
			}

			// Describe template
			return describeOp.Run(cmd.Context(), template, d)
		},
	}

	return cmd
}
