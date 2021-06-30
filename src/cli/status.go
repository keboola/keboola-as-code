package cli

import (
	"github.com/spf13/cobra"
)

const statusShortDescription = `Print info about project directory`
const statusLongDescription = `Command "status"

Print info about current project dir, metadata dir and working dir.
`

func statusCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: statusShortDescription,
		Long:  statusLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := root.ValidateOptions([]string{"projectDirectory"}); err != nil {
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
