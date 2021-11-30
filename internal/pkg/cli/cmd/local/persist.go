package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/persist"
)

const (
	persistShortDescription = `Persist created and deleted configs/rows in manifest`
	persistLongDescription  = `Command "persist"

This command writes the changes from the filesystem to the manifest.
- If you have created a new config/row, this command will write record to the manifest with a unique ID.
- If you have deleted a config/row, this command will delete record from the manifest.

No changes are made to the remote state of the project.

If you also want to change the remote state,
call the "push" command after the "persist" command.
`
)

func PersistCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "persist",
		Short: persistShortDescription,
		Long:  persistLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options := persist.Options{
				DryRun:            d.Options().GetBool(`dry-run`),
				LogUntrackedPaths: true,
			}

			// Persist
			return persist.Run(options, d)
		},
	}

	// Flags
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")

	return cmd
}
