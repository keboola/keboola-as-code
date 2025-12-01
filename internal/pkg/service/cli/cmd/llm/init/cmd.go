package init

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/llm/init"
	createEnvFiles "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/envfiles/create"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Branches        configmap.Value[string] `configKey:"branches" configShorthand:"b" configUsage:"comma separated IDs or name globs, use \"*\" for all"`
	AllowTargetENV  configmap.Value[bool]   `configKey:"allow-target-env" configUsage:"allow usage of KBC_PROJECT_ID and KBC_BRANCH_ID envs for future operations"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: helpmsg.Read(`llm/init/short`),
		Long:  helpmsg.Read(`llm/init/long`),
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
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "llm-init")

			// Init
			return initOp.Run(cmd.Context(), options, d)
		},
	}

	// Flags
	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
