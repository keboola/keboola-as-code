package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

func CreateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: helpmsg.Read(`template/create/short`),
		Long:  helpmsg.Read(`template/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// Require template repository
			if _, err := d.LocalTemplateRepository(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateTemplateOpts(d)
			if err != nil {
				return err
			}

			// Create template
			return createOp.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().String(`id`, ``, "template ID")
	cmd.Flags().String(`name`, ``, "template name")
	cmd.Flags().String(`description`, ``, "template description")
	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`configs`, "c", ``, "comma separated list of {componentId}:{configId}")
	cmd.Flags().StringP(`used-components`, "u", ``, "comma separated list of component ids")
	cmd.Flags().BoolP(`all-configs`, "a", false, "use all configs from the branch")
	cmd.Flags().Bool(`all-inputs`, false, "use all found config/row fields as user inputs")

	return cmd
}
