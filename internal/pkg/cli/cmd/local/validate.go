package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
)

func ValidateCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: helpmsg.Read(`local/validate/short`),
		Long:  helpmsg.Read(`local/validate/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := depsProvider.Dependencies()
			logger := d.Logger()

			// Project is required
			if _, err := d.LocalProject(false); err != nil {
				return err
			}

			// Options
			options := validate.Options{
				ValidateSecrets:    true,
				ValidateJsonSchema: true,
			}

			// Validate
			if err := validate.Run(options, d); err != nil {
				return err
			}

			logger.Info("Everything is good.")
			return nil
		},
	}

	return cmd
}
