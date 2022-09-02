package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func InitCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: helpmsg.Read(`sync/init/short`),
		Long:  helpmsg.Read(`sync/init/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			start := time.Now()
			publicDeps, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			// Require empty dir
			if _, err := publicDeps.EmptyDir(); err != nil {
				return err
			}

			// Authenticate
			if err := publicDeps.Dialogs().AskHostAndToken(publicDeps.Options()); err != nil {
				return err
			}
			projectDeps, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Get init options
			options, err := publicDeps.Dialogs().AskInitOptions(publicDeps.CommandCtx(), projectDeps)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer func() { projectDeps.EventSender().SendCmdEvent(publicDeps.CommandCtx(), start, cmdErr, "sync-init") }()

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
