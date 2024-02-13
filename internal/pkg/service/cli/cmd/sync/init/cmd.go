package init

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type Flags struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	Branches       string `configKey:"branches" configShorthand:"b" configUsage:"comma separated IDs or name globs, use \"*\" for all"`
	CI             bool   `configKey:"ci" configUsage:"generate workflows"`
	CIValidate     bool   `configKey:"ci-validate" configUsage:"create workflow to validate all branches on change"`
	CIPush         bool   `configKey:"ci-push" configUsage:"create workflow to push change in main branch to the project"`
	CIPull         bool   `configKey:"ci-pull" configUsage:"create workflow to sync main branch each hour"`
	CIMainBranch   string `configKey:"ci-main-branch" configUsage:"name of the main branch for push/pull workflows"`
}

func DefaultFlags() *Flags {
	return &Flags{
		CI:           true,
		CIValidate:   true,
		CIPull:       true,
		CIPush:       true,
		CIMainBranch: "main",
		Branches:     "main",
	}
}

func Command(p dependencies.Provider) *cobra.Command {
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

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
