package workspace

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/list"
)

func ListCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `list`,
		Short: helpmsg.Read(`remote/workspace/list/short`),
		Long:  helpmsg.Read(`remote/workspace/list/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			start := time.Now()

			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Ask for host and token if needed
			if err := d.Dialogs().AskHostAndToken(d); err != nil {
				return err
			}

			defer func() { d.EventSender().SendCmdEvent(d.CommandCtx(), start, cmdErr, "remote-list-workspace") }()

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
