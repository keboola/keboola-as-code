package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func InitCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: helpmsg.Read(`sync/init/short`),
		Long:  helpmsg.Read(`sync/init/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			start := time.Now()

			// Require empty dir
			baseDeps := p.BaseDependencies()
			if _, err := baseDeps.EmptyDir(); err != nil {
				return err
			}

			// Ask for host and token if needed
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			// Authenticate
			projectDeps, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Get init options
			options, err := projectDeps.Dialogs().AskInitOptions(projectDeps.CommandCtx(), projectDeps)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer func() { projectDeps.EventSender().SendCmdEvent(projectDeps.CommandCtx(), start, cmdErr, "sync-init") }()

			// Init
			return initOp.Run(projectDeps.CommandCtx(), options, projectDeps)
		},
	}

	// Flags
	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("branches", "b", "main", `comma separated IDs or name globs, use "*" for all`)
	ci.WorkflowsCmdFlags(cmd.Flags())

	return cmd
}
