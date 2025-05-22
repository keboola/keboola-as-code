package init

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	createEnvFiles "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/envfiles/create"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Branches        configmap.Value[string] `configKey:"branches" configShorthand:"b" configUsage:"comma separated IDs or name globs, use \"*\" for all"`
	CI              configmap.Value[bool]   `configKey:"ci" configUsage:"generate workflows"`
	CIValidate      configmap.Value[bool]   `configKey:"ci-validate" configUsage:"create workflow to validate all branches on change"`
	CIPush          configmap.Value[bool]   `configKey:"ci-push" configUsage:"create workflow to push change in main branch to the project"`
	CIPull          configmap.Value[bool]   `configKey:"ci-pull" configUsage:"create workflow to sync main branch each hour"`
	CIMainBranch    configmap.Value[string] `configKey:"ci-main-branch" configUsage:"name of the main branch for push/pull workflows"`
	AllowTargetENV  configmap.Value[bool]   `configKey:"allow-target-env" configUsage:"allow usage of KBC_PROJECT_ID and KBC_BRANCH_ID envs for future operations"`
}

func DefaultFlags() Flags {
	return Flags{
		Branches:       configmap.NewValue("main"),
		CI:             configmap.NewValue(true),
		CIValidate:     configmap.NewValue(true),
		CIPull:         configmap.NewValue(true),
		CIPush:         configmap.NewValue(true),
		CIMainBranch:   configmap.NewValue("main"),
		AllowTargetENV: configmap.NewValue(false),
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

			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Get init options
			options, err := AskInitOptions(cmd.Context(), d.Dialogs(), d, f)
			if err != nil {
				return err
			}

			// Create ENV files
			if err = createEnvFiles.Run(cmd.Context(), d.Fs(), d); err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "sync-init")

			// Init
			return initOp.Run(cmd.Context(), options, d)
		},
	}

	// Flags
	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
