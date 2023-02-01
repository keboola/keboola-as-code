package workspace

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/list"
)

func ListCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `list`,
		Short: helpmsg.Read(`remote/workspace/list/short`),
		Long:  helpmsg.Read(`remote/workspace/list/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-list-workspace")

			err = list.Run(d.CommandCtx(), d)
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")

	return cmd
}
