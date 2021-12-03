package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/status"
)

func StatusCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: helpmsg.Read(`local/status/short`),
		Long:  helpmsg.Read(`local/status/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if _, err := d.ProjectDir(); err != nil {
				return err
			}

			return status.Run(d)
		},
	}

	return cmd
}
