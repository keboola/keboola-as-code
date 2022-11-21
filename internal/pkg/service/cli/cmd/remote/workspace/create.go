package workspace

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

func CreateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/workspace/create/short`),
		Long:  helpmsg.Read(`remote/workspace/create/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			start := time.Now()

			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Ask options
			options, err := d.Dialogs().AskCreateWorkspace(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer func() { d.EventSender().SendCmdEvent(d.CommandCtx(), start, cmdErr, "remote-create-workspace") }()

			// Run operation
			err = create.Run(d.CommandCtx(), options, d)
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("name", "", "name of the workspace")
	cmd.Flags().String("type", "", "type of the workspace")
	cmd.Flags().String("size", "", "size of the workspace")

	return cmd
}
