package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/local/status"
)

const (
	statusShortDescription = `Print info about project directory`
	statusLongDescription  = `Command "status"

Print info about current project dir, metadata dir and working dir.
`
)

func StatusCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: statusShortDescription,
		Long:  statusLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			return status.Run(d)
		},
	}

	return cmd
}
