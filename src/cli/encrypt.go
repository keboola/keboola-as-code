package cli

import "github.com/spf13/cobra"

const encryptShortDescription = "Find unencrypted values in configurations and encrypt them"
const encryptLongDescription = `Command "encrypt"

This command will look for unencrypted values in all local configurations and updates them by encrypting them.
- The encrypted values are properties that begin with '#' and contain string.
- For example {"#someSecretProperty": "secret value"} will be transformed into {"#someSecretProperty": "KBC::ComponentSecure::<encryptedcontent>"}

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`

func encryptCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "encrypt",
		Short: encryptShortDescription,
		Long:  encryptLongDescription,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
