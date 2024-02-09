package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type InitFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Branches       string `mapstructure:"branches" shorthand:"b" usage:"comma separated IDs or name globs, use \"*\" for all"`
	CI             bool   `mapstructure:"ci" usage:"generate workflows"`
	CIValidate     bool   `mapstructure:"ci-validate" usage:"create workflow to validate all branches on change"`
	CIPush         bool   `mapstructure:"ci-push" usage:"create workflow to push change in main branch to the project"`
	CIPull         bool   `mapstructure:"ci-pull" usage:"create workflow to sync main branch each hour"`
	CIMainBranch   string `mapstructure:"ci-main-branch" usage:"name of the main branch for push/pull workflows"`
}

func DefaultInitFlags() *InitFlags {
	return &InitFlags{
		CI:           true,
		CIValidate:   true,
		CIPull:       true,
		CIPush:       true,
		CIMainBranch: "main",
		Branches:     "main",
	}
}

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

	cliconfig.MustGenerateFlags(DefaultInitFlags(), cmd.Flags())

	return cmd
}
