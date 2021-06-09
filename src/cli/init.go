package cli

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"net/url"
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
			// Ask for the host/token, if they were not specified, to make the first step easier
			if len(root.options.ApiHost) == 0 {
				root.options.ApiHost, _ = root.prompt.Ask(&Question{
					Label:       "API host",
					Description: "Please enter Keboola Storage API host, eg. \"keboola.connection.com\".",
					Validator:   apiHostValidator,
				})
			}
			if len(root.options.ApiToken) == 0 {
				root.options.ApiToken, _ = root.prompt.Ask(&Question{
					Label:       "API token",
					Description: "Please enter Keboola Storage API token. The value will be hidden.",
					Hidden:      true,
					Validator:   valueRequired,
				})
			}

			// Validate options
			if err := root.options.Validate([]string{"ApiHost", "ApiToken"}); len(err) > 0 {
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

func apiHostValidator(val interface{}) error {
	str := val.(string)
	if len(str) == 0 {
		return errors.New("value is required")
	} else if _, err := url.Parse(str); err != nil {
		return errors.New("invalid host")
	}
	return nil
}
