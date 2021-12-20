package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/rename"
)

func FixPathsCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-paths",
		Short: helpmsg.Read(`local/fix-paths/short`),
		Long:  helpmsg.Read(`local/fix-paths/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			if _, err := d.ProjectDir(); err != nil {
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
