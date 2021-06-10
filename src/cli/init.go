package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

const shortDescription = `Init directory and perform the first pull`
const longDescription = `Command "init"

Initialize local project's directory
and first time sync project from the Keboola Connection.

You will be asked to enter the Storage API host
and Storage API token from your project.
You can also enter these values
as flags or environment variables.`

func initCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: shortDescription,
		Long:  longDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Ask for the host/token, if not specified -> to make the first step easier
			root.options.AskUser(root.prompt, "ApiHost")
			root.options.AskUser(root.prompt, "ApiToken")

			// Validate options
			if err := root.options.Validate([]string{"ApiHost", "ApiToken"}); len(err) > 0 {
				root.logger.Warn("Invalid parameters: \n", err)
				return fmt.Errorf("invalid parameters, see output above")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate token and get API
			_, err := root.NewStorageApi()
			if err != nil {
				return err
			}

			// TODO
			return fmt.Errorf("TODO")
		},
	}

	return cmd
}
