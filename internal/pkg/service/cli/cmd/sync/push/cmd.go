package push

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Force           configmap.Value[bool]   `configKey:"force" configUsage:"enable deleting of remote objects"`
	DryRun          configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
	Encrypt         configmap.Value[bool]   `configKey:"encrypt" configUsage:"encrypt unencrypted values before push"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `push ["change description"]`,
		Short: helpmsg.Read(`sync/push/short`),
		Long:  helpmsg.Read(`sync/push/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Command must be used in project directory
			_, _, err := p.BaseScope().FsInfo().ProjectDir(cmd.Context())
			if err != nil {
				return err
			}

			f := Flags{}
			if err = p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Get local project
			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(cmd.Context(), loadState.PushOptions(), d)
			if err != nil {
				return err
			}

			// Change description - optional arg
			changeDescription := "Updated from #KeboolaCLI"
			if len(args) > 0 {
				changeDescription = args[0]
			}

			// Options
			options := push.Options{
				Encrypt:           f.Encrypt.Value,
				DryRun:            f.DryRun.Value,
				AllowRemoteDelete: f.Force.Value,
				LogUntrackedPaths: true,
				ChangeDescription: changeDescription,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "sync-push")

			// Push
			return push.Run(cmd.Context(), projectState, options, d)
		},
	}

	// Flags
	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
