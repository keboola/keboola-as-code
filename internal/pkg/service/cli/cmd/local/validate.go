package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func ValidateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: helpmsg.Read(`local/validate/short`),
		Long:  helpmsg.Read(`local/validate/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := validate.Options{
				ValidateSecrets:    true,
				ValidateJSONSchema: true,
			}

			// Validate
			if err := validate.Run(d.CommandCtx(), projectState, options, d); err != nil {
				return err
			}

			d.Logger().Info("Everything is good.")
			return nil
		},
	}

	return cmd
}
