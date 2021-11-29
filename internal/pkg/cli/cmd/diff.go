package cmd

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/sync/diff/printDiff"
)

const (
	diffShortDescription = `Print differences between local and remote state`
	diffLongDescription  = `Command "diff"

Print differences between local and remote state.
`
)

func DiffCommand(root *RootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: diffShortDescription,
		Long:  diffLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := root.Deps

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options := printDiff.Options{
				PrintDetails:      d.Options().GetBool(`details`),
				LogUntrackedPaths: true,
			}

			// Print diff
			results, err := printDiff.Run(options, d)
			if err != nil {
				return err
			}

			// Print info about --details flag
			if !options.PrintDetails && results.HasNotEqualResult {
				root.Logger.Info()
				root.Logger.Info(`Use --details flag to list the changed fields.`)
			}
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("details", false, "print changed fields")

	return cmd
}
