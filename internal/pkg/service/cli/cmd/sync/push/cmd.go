package push

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	Force   bool `configShorthand:"force" configUsage:"enable deleting of remote objects"`
	DryRun  bool `configShorthand:"dry-run" configUsage:"print what needs to be done"`
	Encrypt bool `configShorthand:"encrypt" configUsage:"encrypt unencrypted values before push"`
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

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Get local project
			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.PushOptions(), d)
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
				Encrypt:           d.Options().GetBool("encrypt"),
				DryRun:            d.Options().GetBool("dry-run"),
				AllowRemoteDelete: d.Options().GetBool("force"),
				LogUntrackedPaths: true,
				ChangeDescription: changeDescription,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "sync-push")

			// Push
			return push.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
