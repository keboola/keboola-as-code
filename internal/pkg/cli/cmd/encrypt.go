package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/encrypt"
)

const (
	encryptShortDescription = "Find unencrypted values in configurations and encrypt them"
	encryptLongDescription  = `Command "encrypt"

This command searches for unencrypted values in al local configurations and encrypts them.
- The encrypted values are properties that begin with '#' and contain string.
- For example {"#someSecretProperty": "secret value"} will be transformed into {"#someSecretProperty": "KBC::ProjectSecure::<encryptedcontent>"}

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`
)

func EncryptCommand(root *RootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "encrypt",
		Short: encryptShortDescription,
		Long:  encryptLongDescription,
		RunE: func(cmd *cobra.Command, args []string) error {
			d := root.Deps

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options := encrypt.Options{
				DryRun: d.Options().GetBool(`dry-run`),
			}

			// Encrypt
			return encrypt.Run(options, d)
		},
	}
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
