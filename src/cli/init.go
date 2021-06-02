package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

const shortDescription = `Init directory and perform the first pull`
const longDescription = `Command "init"

Running the "init" command, the project structure
and component configurations are first time synchronized
from the Keboola Connection to the working directory.

The project is defined by the Storage API URL and token.
They can be entered via ENV variables, .env file or as an argument.`

func (c *commander) initCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: shortDescription,
		Long:  longDescription,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("TODO")
		},
	}

	return cmd
}
