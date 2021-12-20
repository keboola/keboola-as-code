package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func EditCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: helpmsg.Read(`template/edit/short`),
		Long:  helpmsg.Read(`template/edit/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Require template repository dir
			if _, err := d.TemplateRepositoryDir(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateTemplateOpts(d)
			if err != nil {
				return err
			}

			// nolint: forbidigo
			fmt.Printf("%#v\n\n", options)

			return fmt.Errorf(`not implemented`)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`configs`, "c", ``, "comma separated list of {componentId}:{configId}")
	cmd.Flags().BoolP(`all-configs`, "a", false, "use all configs from the branch")

	return cmd
}
