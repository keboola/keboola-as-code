package cli

import (
	"github.com/spf13/cobra"
)

const persistShortDescription = `Persist created and deleted configs/rows in manifest`
const persistLongDescription = `Command "persist"

This command writes the changes from the filesystem to the manifest.
- If you have created a new config/row, this command will write record to the manifest with a unique ID.
- If you have deleted a config/row, this command will delete record from the manifest.

No changes are made to the remote state of the project.

If you also want to change the remote state,
call the "push" command after the "persist" command.
`

func persistCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "persist",
		Short: persistShortDescription,
		Long:  persistLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			return nil
		},
	}

	return cmd
}
