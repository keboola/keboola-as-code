package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

const validateShortDescription = `Validate the local project dir`
const validateLongDescription = `Command "validate"

Validate existence and contents of all files in the local project dir.
For components with a JSON schema, the content must match the schema.
`

func validateCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: validateShortDescription,
		Long:  validateLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiHost", "ApiToken"}); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			return fmt.Errorf("TODO")
		},
	}

	return cmd
}
