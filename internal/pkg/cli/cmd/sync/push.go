package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/sync/push"
)

const (
	pushShortDescription = `Push configurations to the Keboola Connection project`
	pushLongDescription  = `Command "push"

Push configurations to the Keboola Connection project.
Project's state will be overwritten to match the local files.

You can specify an optional ["change description"].
It will be visible in the config's versions.

You can use the "--dry-run" flag to see
what needs to be done without modifying the project's state.
`
)

func PushCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `push ["change description"]`,
		Short: pushShortDescription,
		Long:  pushLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()
			logger := d.Logger()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Change description - optional arg
			changeDescription := "Updated from #KeboolaCLI"
			if len(args) > 0 {
				changeDescription = args[0]
			}
			logger.Debugf(`Change description: "%s"`, changeDescription)

			// Options
			options := push.Options{
				Encrypt:           d.Options().GetBool("encrypt"),
				DryRun:            d.Options().GetBool("dry-run"),
				AllowRemoteDelete: d.Options().GetBool("force"),
				LogUntrackedPaths: true,
				ChangeDescription: changeDescription,
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "push")
				}()
			} else {
				return err
			}

			// Push
			return push.Run(options, d)
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "enable deleting of remote objects")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	cmd.Flags().Bool("encrypt", false, "encrypt unencrypted values before push")
	return cmd
}
