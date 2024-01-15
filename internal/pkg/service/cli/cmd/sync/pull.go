package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func PullCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: helpmsg.Read(`sync/pull/short`),
		Long:  helpmsg.Read(`sync/pull/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Command must be used in project directory
			_, _, err := p.BaseScope().FsInfo().ProjectDir(cmd.Context())
			if err != nil {
				return err
			}

			// Authentication
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Get local project
			logger := d.Logger()
			force := d.Options().GetBool(`force`)
			prj, _, err := d.LocalProject(cmd.Context(), force)
			if err != nil {
				if !force && errors.As(err, &project.InvalidManifestError{}) {
					logger.Info(cmd.Context())
					logger.Info(cmd.Context(), "Use --force to override the invalid local state.")
				}
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.PullOptions(force), d)
			if err != nil {
				if !force && errors.As(err, &loadState.InvalidLocalStateError{}) {
					logger.Info(cmd.Context())
					logger.Info(cmd.Context(), "Use --force to override the invalid local state.")
				}
				return err
			}

			// Options
			options := pull.Options{
				DryRun:            d.Options().GetBool(`dry-run`),
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "sync-pull")

			// Pull
			return pull.Run(cmd.Context(), projectState, options, d)
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
