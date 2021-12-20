package sync

import (
	"errors"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/sync/pull"
)

func PullCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: helpmsg.Read(`sync/pull/short`),
		Long:  helpmsg.Read(`sync/pull/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()
			logger := d.Logger()

			// Metadata directory is required
			if _, err := d.ProjectDir(); err != nil {
				return err
			}

			// Options
			options := pull.Options{
				DryRun:            d.Options().GetBool(`dry-run`),
				Force:             d.Options().GetBool(`force`),
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "pull")
				}()
			} else {
				return err
			}

			// Pull
			if err := pull.Run(options, d); err != nil && errors.As(err, &loadState.InvalidLocalStateError{}) {
				logger.Info()
				logger.Info("Use --force to override the invalid local state.")
				return err
			} else if err != nil {
				return err
			}
			return nil
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
