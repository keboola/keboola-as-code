package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

const shortDescription = `Init directory and perform the first pull`
const longDescription = `Command "init"

Project structure and component configurations are first time synchronized
from the Keboola Connection to the working directory.

The project is defined by the Storage API URL and token.
They can be entered via ENV variables, .env file or as an argument.`

func initCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: shortDescription,
		Long:  longDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Validate options
			if err := root.options.Validate([]string{"ApiUrl", "ApiToken"}); len(err) > 0 {
				root.logger.Warn("Invalid parameters: \n", err)
				return fmt.Errorf("invalid parameters, see output above")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO
			return fmt.Errorf("TODO")
		},
	}

	return cmd
}
