package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

const (
	statusShortDescription = `Print info about project directory`
	statusLongDescription  = `Command "status"

Print info about current project dir, metadata dir and working dir.
`
)

func statusCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: statusShortDescription,
		Long:  statusLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Validate
			if !root.fs.IsDir(filesystem.MetadataDir) {
				root.logger.Infof(`Start by running the "init" sub-command in an empty directory.`)
				return fmt.Errorf("none of this and parent directories is project dir")
			}

			// Load manifest
			projectManifest, err := manifest.LoadManifest(root.fs, zap.NewNop().Sugar())
			if err != nil {
				return err
			}

			root.logger.Infof("Project directory:  %s", root.fs.BasePath())
			root.logger.Infof("Working directory:  %s", root.fs.WorkingDir())
			root.logger.Infof("Manifest path:      %s", projectManifest.Path())
			return nil
		},
	}

	return cmd
}
