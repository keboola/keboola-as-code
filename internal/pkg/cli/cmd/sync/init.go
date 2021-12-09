package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/sync/init"
)

func InitCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: helpmsg.Read(`sync/init/short`),
		Long:  helpmsg.Read(`sync/init/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()

			// Require empty dir
			if _, err := d.EmptyDir(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskInitOptions(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "init")
				}()
			} else {
				return err
			}

			// Init
			return initOp.Run(options, d)
		},
	}

	// Flags
	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("branches", "b", "main", `comma separated IDs or name globs, use "*" for all`)
	ci.WorkflowsCmdFlags(cmd.Flags())

	return cmd
}
