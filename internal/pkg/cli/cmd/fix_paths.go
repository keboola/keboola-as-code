package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/rename"
)

const (
	fixPathsShortDescription = `Normalize all local paths`
	fixPathsLongDescription  = `Command "fix-paths"

Manifest file ".keboola/manifest.json" contains a naming for all local paths.

With this command you can rename all existing paths
to match the configured naming (eg. if the naming has been changed).
`
)

func FixPathsCommand(root *RootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-paths",
		Short: fixPathsShortDescription,
		Long:  fixPathsLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := root.Deps

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options := rename.Options{
				DryRun:   d.Options().GetBool(`dry-run`),
				LogEmpty: true,
			}

			// Rename
			_, err = rename.Run(options, d)
			return err
		},
	}

	// Flags
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
