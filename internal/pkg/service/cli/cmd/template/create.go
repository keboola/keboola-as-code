package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

func CreateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: helpmsg.Read(`template/create/short`),
		Long:  helpmsg.Read(`template/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in template repository
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateTemplateOpts(cmd.Context(), d)
			if err != nil {
				return err
			}

			// Create template
			return createOp.Run(cmd.Context(), options, d)
		},
	}

	createFlags := CreateFlags{}
	_ = cliconfig.GenerateFlags(createFlags, cmd.Flags())

	return cmd
}
