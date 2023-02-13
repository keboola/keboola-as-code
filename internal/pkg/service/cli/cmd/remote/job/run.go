package job

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/job/run"
)

func RunCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `run [branch/]component/config`,
		Short: helpmsg.Read(`remote/job/run/short`),
		Long:  helpmsg.Read(`remote/job/run/long`),
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
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

			options := run.RunOptions{}
			err = options.Parse(d.Options(), args, d.ProjectFeatures().Has("queuev2"))
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-job-run")

			return run.Run(d.CommandCtx(), options, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().Bool("async", false, "do not wait for job to finish")
	cmd.Flags().String("timeout", "2m", "how long to wait for job to finish")

	return cmd
}
