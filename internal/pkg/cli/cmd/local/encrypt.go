package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
)

func EncryptCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "encrypt",
		Short: helpmsg.Read(`local/encrypt/short`),
		Long:  helpmsg.Read(`local/encrypt/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			if _, err := d.ProjectDir(); err != nil {
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
