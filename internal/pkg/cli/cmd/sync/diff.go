package sync

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/sync/diff/printDiff"
)

func DiffCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: helpmsg.Read(`sync/diff/short`),
		Long:  helpmsg.Read(`sync/diff/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := depsProvider.Dependencies()
			logger := d.Logger()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if _, err := d.ProjectDir(); err != nil {
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
				logger.Info()
				logger.Info(`Use --details flag to list the changed fields.`)
			}
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("details", false, "print changed fields")

	return cmd
}
