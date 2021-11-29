package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/validate"
)

const (
	validateShortDescription = `Validate the local project directory`
	validateLongDescription  = `Command "validate"

Validate existence and contents of all files in the local project dir.
For components with a JSON schema, the content must match the schema.
`
)

func ValidateCommand(root *RootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: validateShortDescription,
		Long:  validateLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := root.Deps
			logger := d.Logger()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
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
