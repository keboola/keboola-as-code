package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func InitCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: helpmsg.Read(`sync/init/short`),
		Long:  helpmsg.Read(`sync/init/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Require empty dir
			if _, err := p.BaseScope().EmptyDir(cmd.Context()); err != nil {
				return err
			}

			// Get dependencies
			projectDeps, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Get init options
			options, err := projectDeps.Dialogs().AskInitOptions(cmd.Context(), projectDeps)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer projectDeps.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "sync-init")

			// Init
			return initOp.Run(cmd.Context(), options, projectDeps)
		},
	}

	initFlags := InitFlags{
		CI:           true,
		CIValidate:   true,
		CIPull:       true,
		CIPush:       true,
		CIMainBranch: "main",
		Branches:     "main",
	}
	_ = cliconfig.GenerateFlags(initFlags, cmd.Flags())

	return cmd
}
