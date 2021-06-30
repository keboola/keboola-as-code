package cli

import (
	"github.com/spf13/cobra"
)

const diffShortDescription = `Print differences between local and remote state`
const diffLongDescription = `Command "diff"

Print differences between local and remote state.
`

func diffCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: diffShortDescription,
		Long:  diffLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiToken"}); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			return nil
		},
	}

	return cmd
}
