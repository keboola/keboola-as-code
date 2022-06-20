package sync

import (
	"errors"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func PullCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: helpmsg.Read(`sync/pull/short`),
		Long:  helpmsg.Read(`sync/pull/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := p.Dependencies()
			start := time.Now()
			logger := d.Logger()

			// Load project state
			force := d.Options().GetBool(`force`)
			prj, err := d.LocalProject(force)
			if err != nil {
				if !force && errors.As(err, &project.InvalidManifestError{}) {
					logger.Info()
					logger.Info("Use --force to override the invalid local state.")
				}
				return err
			}
			projectState, err := prj.LoadState(loadState.PullOptions(force))
			if err != nil {
				if !force && errors.As(err, &loadState.InvalidLocalStateError{}) {
					logger.Info()
					logger.Info("Use --force to override the invalid local state.")
				}
				return err
			}

			// Options
			options := pull.Options{
				DryRun:            d.Options().GetBool(`dry-run`),
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() { eventSender.SendCmdEvent(d.Ctx(), start, cmdErr, "sync-pull") }()
			} else {
				return err
			}

			// Pull
			return pull.Run(projectState, options, d)
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
